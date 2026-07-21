import json
import datetime
import logging

from seaqa_indexer.handlers.base_handler import ConnectionHandler
from seaqa_indexer.utils.constants import ConnectionType
from seaqa_indexer.utils.discord_api import DiscordAPI
from seaqa_indexer.seadb_models.discord_seadb_api import DiscordSeaDBAPI
from seaqa_indexer.index.utils import gen_index_name_by_type
from seaqa_indexer.utils.constants import IndexStatus
from seaqa_indexer.utils import need_update_connection_last_sync_count, time_to_utc_time
from seaqa_indexer.index.index_store.discord_index import DiscordIndex
from seaqa_indexer.utils.constants import DataEventType

from seaqa_indexer.seadb_models.models import SchemaTables


logger = logging.getLogger(__name__)


class DiscordHandler(ConnectionHandler):
    """Handler for DISCORD connection type."""

    connection_type = ConnectionType.DISCORD.value
    BATCH_SIZE = 50

    def gen_table_name(self, connection_id):
        return SchemaTables.DISCORD_THREADS.table_name(connection_id=connection_id)

    def gen_messages_table_name(self, connection_id):
        return SchemaTables.DISCORD_THREAD_MESSAGES.table_name(connection_id=connection_id)

    def get_search_param(self, seasearch_api, index_name, keyword, start, size, time_from, time_to):
        discord_index = DiscordIndex(seasearch_api)
        return discord_index.get_search_param(index_name, keyword, start, size, time_from, time_to)

    def delete_index_by_connection_id(self, seasearch_api, connection_id):
        index_name = gen_index_name_by_type(connection_id, self.connection_type)
        discord_index = DiscordIndex(seasearch_api)
        discord_index.delete_index_by_index_name(index_name)

        # summary vector index
        self.delete_summary_vector_index_by_connection_id(seasearch_api, connection_id)

    def format_vector_search_result(self, record, connection, result):
        connection_config = json.loads(connection.config)
        channel_id = connection_config.get('channel_id')
        thread_id = record.get('thread_id')
        guild_id = connection_config.get('guild_id')
        message_url = f'https://discord.com/channels/{guild_id}/{channel_id}/{thread_id}'

        return {
            'title': record.get('title', ''),
            'thread_id': record.get('thread_id', ''),
            'url': message_url,
        }

    def process_ai(self, seadb_api, storage, connection_id, project_uuid):
        page_size = 100
        updates = []
        table_name = self.gen_table_name(connection_id)
        discord_seadb_api = DiscordSeaDBAPI(project_uuid)

        ai_processing_count = 0
        while True:
            rows = discord_seadb_api.get_need_ai_threads(connection_id, page_size)
            if not rows:
                break

            for row in rows:
                pk = row.get('_pk')
                thread_id = row.get('thread_id', '')
                title = row.get('title', '')
                first_message = discord_seadb_api.get_thread_first_message(connection_id, thread_id)
                first_content = first_message.get('content', '') or ''
                body = first_content
                full_content = f"Title: {title}\n\nBody: {body}\n"
                summary = self.seaqa_ai_api.generate_summary(full_content, ConnectionType.DISCORD.value, project_uuid)
                vector = self.seaqa_ai_api.generate_embedding(summary, project_uuid)
                updates.append({
                    "pk": pk,
                    "row": {
                        "ai_summary": summary,
                        "ai_summary_vector": vector,
                        "ai_processed_time": datetime.datetime.now(datetime.UTC).isoformat()
                    }
                })
                ai_processing_count += 1
                if len(updates) >= self.BATCH_SIZE:
                    seadb_api.update_rows(project_uuid, table_name, updates)
                    updates = []
            if updates:
                seadb_api.update_rows(project_uuid, table_name, updates)
        return ai_processing_count

    def update_index(self, project_connection, seasearch_api, seaqa_db, since):
        connection_id = project_connection.id
        connection_type = project_connection.type
        project_uuid = project_connection.project_uuid
        connection_status = json.loads(project_connection.status)

        discord_seadb_api = DiscordSeaDBAPI(project_uuid)
        discord_index = DiscordIndex(seasearch_api)

        connection_status['last_index_status'] = IndexStatus.INDEXING
        seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
        index_name = gen_index_name_by_type(connection_id, connection_type)
        discord_index.create_index_if_missing(index_name)

        index_count = discord_index.update(index_name, connection_id, connection_status, seaqa_db, discord_seadb_api, since)
        indexed_at = datetime.datetime.now(datetime.UTC)
        connection_status['last_indexed_count'] = index_count
        connection_status['last_index_status'] = IndexStatus.COMPLETED
        seaqa_db.update_connection(connection_id, indexed_at, json.dumps(connection_status))

    def sync(self, connection_id, project_uuid, config, old_last_sync_time,
             connection_status, seaqa_db, sync_logger):
        guild_id = config.get('guild_id', '')
        channel_id = config.get('channel_id', '')
        # SYNC_SELECT fields store the full option object; extract the raw value
        if isinstance(channel_id, dict):
            channel_id = channel_id.get('value', '')
        bot_token = config.get('bot_token', '')

        if not bot_token:
            logger.error(f'Discord bot_token is not configured in settings for connection {connection_id}')
            return None, None, None, None

        if not channel_id:
            logger.error(f'Discord channel_id is missing for connection {connection_id}')
            return None, None, None, None

        discord_api = DiscordAPI(bot_token)
        discord_seadb_api = DiscordSeaDBAPI(project_uuid)

        sync_count, total_count = self._sync_discord(
            connection_id, channel_id, guild_id, old_last_sync_time,
            connection_status, discord_api, discord_seadb_api, seaqa_db
        )
        return sync_count, total_count, False, config

    def clean_deleted_records(self, seadb_api, project_uuid, connection_id, cutoff_time):
        """Clean deleted discord thread records and their messages."""
        page_size = 1000
        table_name = self.gen_table_name(connection_id)
        messages_table_name = self.gen_messages_table_name(connection_id)
        total_deleted = 0

        while True:
            sql = f"""
                SELECT _pk, thread_id FROM `{table_name}`
                WHERE deleted = true AND record_modified_time < '{cutoff_time}'
                LIMIT {page_size}
            """
            result = seadb_api.query_rows(project_uuid, sql)
            rows = result.get('results', [])
            if not rows:
                break

            deleted_pks = [row.get('_pk') for row in rows]
            deleted_thread_ids = [row.get('thread_id') for row in rows]

            seadb_api.delete_rows(project_uuid, table_name, deleted_pks)
            for thread_id in deleted_thread_ids:
                delete_sql = f"DELETE FROM `{messages_table_name}` WHERE thread_id = '{thread_id}'"
                seadb_api.query_rows(project_uuid, delete_sql)

            total_deleted += len(deleted_pks)

            if len(rows) < page_size:
                break

        return total_deleted

    @staticmethod
    def _datetime_to_snowflake(dt):
        """Convert a datetime to a Discord snowflake ID string.

        Discord snowflakes encode the creation timestamp:
            snowflake = (timestamp_ms - DISCORD_EPOCH) << 22
        """
        if dt is None:
            return None
        DISCORD_EPOCH_MS = 1420070400000
        timestamp_ms = int(dt.timestamp() * 1000)
        return str((timestamp_ms - DISCORD_EPOCH_MS) << 22)

    @staticmethod
    def _fetch_all_archived_threads(discord_api, channel_id, last_sync_time=None):
        """Fetch archived threads, stopping when the oldest thread in a page
        passes the last_sync_time cutoff.

        Threads are returned in reverse chronological order by archive_timestamp,
        so threads[-1] is the oldest in the page."""
        all_threads = []
        before = None
        while True:
            threads, has_more = discord_api.fetch_archived_forum_threads(
                channel_id, before=before
            )
            if not threads:
                break

            all_threads.extend(threads)

            if last_sync_time:
                oldest_archive_time = time_to_utc_time(
                    threads[-1].get('thread_metadata', {}).get('archive_timestamp')
                )
                if oldest_archive_time <= last_sync_time:
                    break

            if not has_more:
                break
            before = threads[-1].get('thread_metadata', {}).get('archive_timestamp')
            if not before:
                break
        return all_threads

    @staticmethod
    def _fetch_all_messages(discord_api, thread_id, after=None):
        all_messages = []
        before = None

        while True:
            messages = discord_api.fetch_channel_messages(
                thread_id, before=before, after=after
            )
            if not messages:
                break

            all_messages.extend(messages)

            if len(messages) < 100:
                break

            before = messages[-1].get('id')

        return all_messages

    def _sync_discord(self, connection_id, channel_id, guild_id, last_sync_time,
                            connection_status, discord_api, discord_seadb_api, seaqa_db):
        """Sync a forum channel: each thread is a record, all messages go to the messages table."""
        sync_count = 0
        added_count = 0
        updated_count = 0
        _status_last_update_time = datetime.datetime.now()
        _status_last_update_count = 0

        is_first_sync = not last_sync_time

        if last_sync_time:
            last_sync_time = time_to_utc_time(str(last_sync_time))

        after_snowflake = self._datetime_to_snowflake(last_sync_time)

        active_threads = discord_api.fetch_forum_threads(guild_id, channel_id)

        archived_threads = self._fetch_all_archived_threads(discord_api, channel_id, last_sync_time)

        all_threads = active_threads + archived_threads
        all_threads.sort(
            key=lambda t: int(t.get('last_message_id') or '0'), reverse=True
        )

        for thread in all_threads:
            thread_id = thread.get('id')
            last_message_id = thread.get('last_message_id')

            # Thread-level skip — threads are sorted by last_message_id DESC,
            # so once we pass the cutoff, no remaining threads need processing.
            if not is_first_sync and last_message_id:
                if int(last_message_id) <= int(after_snowflake):
                    break

            thread_messages = self._fetch_all_messages(
                discord_api, thread_id, after=after_snowflake
            )

            if not thread_messages:
                continue

            existing_thread = discord_seadb_api.get_thread_by_thread_id(
                connection_id, thread_id
            )
            is_new_record = not existing_thread

            discord_seadb_api.save_or_update_thread_to_db(
                connection_id, thread, existing_thread
            )
            if is_new_record:
                added_count += 1
            else:
                updated_count += 1

            # All messages (including the opening post) go to the messages table.
            message_result = discord_seadb_api.add_or_update_messages(
                connection_id, thread_id, thread_messages
            )

            event_type = (
                DataEventType.DISCORD_THREAD_UPDATED.value
                if not is_new_record
                else DataEventType.DISCORD_THREAD_ADDED.value
            )
            sync_count += 1

            self.publish_synced_record_events(
                get_latest_row=lambda mid=thread_id: discord_seadb_api.get_thread_by_thread_id(
                    connection_id, mid
                ),
                project_uuid=discord_seadb_api.base_id,
                connection_id=connection_id,
                item_id=thread_id,
                old_row=existing_thread,
                state_event_type=event_type,
                additional_items=message_result.get('added_messages', []),
                additional_event_type=DataEventType.DISCORD_THREAD_MESSAGE_ADDED.value,
                state_event_ignored_fields=(
                    'modified_time', 'record_modified_time', 'sync_time'
                ),
                log_object_name='forum thread',
            )

            should, new_time, new_count = need_update_connection_last_sync_count(
                sync_count, _status_last_update_time, _status_last_update_count
            )
            if should:
                connection_status['last_sync_count'] = sync_count
                seaqa_db.update_connection_status(
                    connection_id, json.dumps(connection_status)
                )
                _status_last_update_time = new_time
                _status_last_update_count = new_count

        connection_status['last_sync_changes'] = {
            'added': added_count,
            'deleted': 0,
            'updated': updated_count,
        }
        total_count = discord_seadb_api.get_thread_count(connection_id)
        return sync_count, total_count
