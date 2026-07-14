import json
import datetime
import logging
import time

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
        return SchemaTables.DISCORD_MESSAGES.table_name(connection_id=connection_id)

    def gen_replies_table_name(self, connection_id):
        return SchemaTables.DISCORD_MESSAGE_REPLIES.table_name(connection_id=connection_id)

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
        message_id = record.get('message_id')
        guild_id = connection_config.get('guild_id')
        message_url = f'https://discord.com/channels/{guild_id}/{channel_id}/{message_id}'

        return {
            'title': record.get('title', ''),
            'message_id': record.get('message_id', ''),
            'url': message_url,
        }

    def process_ai(self, seadb_api, storage, connection_id, project_uuid):
        page_size = 100
        updates = []
        table_name = self.gen_table_name(connection_id)
        discord_seadb_api = DiscordSeaDBAPI(project_uuid)

        ai_processing_count = 0
        while True:
            rows = discord_seadb_api.get_need_ai_messages(connection_id, page_size)
            if not rows:
                break

            for row in rows:
                pk = row.get('_pk')
                message_id = row.get('message_id', '')
                title = row.get('title', '')
                content = row.get('content', '') or ''
                reply_content = discord_seadb_api.get_message_replies_for_index(connection_id, message_id)
                body = content + (' ' + reply_content if reply_content else '')
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

        # Detect forum channels (type 15) — they require thread iteration
        try:
            channel_info = discord_api.get_channel(channel_id)
            channel_type = channel_info.get('type', 0)
        except Exception as e:
            logger.error(f'Failed to get channel info for {channel_id}: {e}')
            return None, None, None, None

        if channel_type == 15:
            sync_count, total_count = self._sync_discord_forum(
                connection_id, channel_id, guild_id, old_last_sync_time,
                connection_status, discord_api, discord_seadb_api, seaqa_db
            )
        else:
            sync_count, total_count = self._sync_discord(
                connection_id, channel_id, guild_id, old_last_sync_time,
                connection_status, discord_api, discord_seadb_api, seaqa_db
            )
        return sync_count, total_count, False, config

    def clean_deleted_records(self, seadb_api, project_uuid, connection_id, cutoff_time):
        """Clean deleted discord message records and their replies."""
        page_size = 1000
        table_name = self.gen_table_name(connection_id)
        replies_table_name = self.gen_replies_table_name(connection_id)
        total_deleted = 0

        while True:
            sql = f"""
                SELECT _pk, message_id FROM `{table_name}`
                WHERE deleted = true AND record_modified_time < '{cutoff_time}'
                LIMIT {page_size}
            """
            result = seadb_api.query_rows(project_uuid, sql)
            rows = result.get('results', [])
            if not rows:
                break

            deleted_pks = [row.get('_pk') for row in rows]
            deleted_message_ids = [row.get('message_id') for row in rows]

            seadb_api.delete_rows(project_uuid, table_name, deleted_pks)
            for message_id in deleted_message_ids:
                delete_sql = f"DELETE FROM `{replies_table_name}` WHERE message_id = '{message_id}'"
                seadb_api.query_rows(project_uuid, delete_sql)

            total_deleted += len(deleted_pks)

            if len(rows) < page_size:
                break

        return total_deleted

    def _sync_discord(self, connection_id, channel_id, guild_id, last_sync_time,
                      connection_status, discord_api, discord_seadb_api, seaqa_db):
        should_stop = False
        sync_count = 0
        added_count = 0
        updated_count = 0
        synced_thread_ids = set()  # Track threads already synced via message loop
        _status_last_update_time = datetime.datetime.now()
        _status_last_update_count = 0
        before = None  # Cursor for pagination (Discord uses message IDs as cursors)

        if last_sync_time:
            last_sync_time = time_to_utc_time(str(last_sync_time))

        while True:
            messages = discord_api.fetch_channel_messages(channel_id, before=before)

            if not messages:
                break

            for msg_data in messages:
                # Skip system messages that have no user content.
                # type 0 = DEFAULT (normal message), type 19 = REPLY (inline reply to
                # another message — has real content, must be synced).
                # type 18 = THREAD_CREATED, type 21 = THREAD_STARTER_MESSAGE, etc.
                msg_type = msg_data.get('type', 0)
                if msg_type not in (0, 19):
                    continue

                message_timestamp = msg_data.get('edited_timestamp') or msg_data.get('timestamp')
                if message_timestamp:
                    message_time = time_to_utc_time(message_timestamp)
                else:
                    continue

                # For messages with a thread, use the thread's last_message_id
                # (snowflake-encoded timestamp) to determine real last activity time.
                # Thread replies can be added long after the parent message was created.
                thread = msg_data.get('thread')
                if thread:
                    last_message_id = thread.get('last_message_id')
                    if last_message_id:
                        last_activity_ms = (int(last_message_id) >> 22) + 1420070400000
                        last_activity_time = datetime.datetime.fromtimestamp(
                            last_activity_ms / 1000, tz=datetime.timezone.utc
                        )
                        message_time = max(message_time, last_activity_time)

                # Stop if we've reached messages older than last sync time
                if last_sync_time and message_time <= last_sync_time:
                    should_stop = True
                    break

                message_id = msg_data.get('id')

                # Check if message already exists in DB
                existing_message = discord_seadb_api.get_message_by_message_id(connection_id, message_id)
                message_changed = True
                if existing_message:
                    existing_modified_time = time_to_utc_time(str(existing_message.get('modified_time')))
                    if message_time <= existing_modified_time:
                        message_changed = False

                # Enrich msg_data with thread_id and thread title for storage
                if thread:
                    msg_data['thread_id'] = thread.get('id')
                    if thread.get('name'):
                        msg_data['title'] = thread['name']
                else:
                    msg_data['thread_id'] = None

                if message_changed:
                    message_state_event_type = (
                        DataEventType.DISCORD_MESSAGE_UPDATED.value
                        if existing_message
                        else DataEventType.DISCORD_MESSAGE_ADDED.value
                    )
                    primary_key = existing_message.get('_pk')
                    if primary_key:
                        discord_seadb_api.update_message(connection_id, primary_key, msg_data)
                        updated_count += 1
                    else:
                        discord_seadb_api.insert_message(connection_id, msg_data)
                        added_count += 1

                # Always sync thread replies — they can change even when
                # the parent message content hasn't been edited
                if thread:
                    thread_id = thread.get('id')
                    if thread_id:
                        synced_thread_ids.add(thread_id)
                        thread_messages = self._fetch_all_messages(
                            discord_api, thread_id, last_sync_time=last_sync_time
                        )
                        if thread_messages:
                            reply_result = discord_seadb_api.update_message_replies(
                                connection_id, message_id, thread_messages
                            )
                            if reply_result['added'] + reply_result['updated'] + reply_result['deleted'] > 0:
                                message_changed = True

                # For type 19 REPLY messages (inline replies), store the parent-child
                # relationship so they appear as replies to the referenced message.
                if msg_type == 19:
                    message_reference = msg_data.get('message_reference', {})
                    parent_message_id = message_reference.get('message_id')
                    if parent_message_id:
                        discord_seadb_api.upsert_inline_reply(connection_id, parent_message_id, msg_data)

                if message_changed:
                    sync_count += 1

                    self.publish_synced_record_events(
                        get_latest_row=lambda: discord_seadb_api.get_message_by_message_id(connection_id, message_id),
                        project_uuid=discord_seadb_api.base_id,
                        connection_id=connection_id,
                        item_id=message_id,
                        old_row=existing_message,
                        state_event_type=message_state_event_type,
                        state_event_ignored_fields=('modified_time', 'record_modified_time', 'sync_time'),
                        log_object_name='message',
                    )

                should, new_time, new_count = need_update_connection_last_sync_count(
                    sync_count, _status_last_update_time, _status_last_update_count
                )
                if should:
                    connection_status['last_sync_count'] = sync_count
                    seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
                    _status_last_update_time = new_time
                    _status_last_update_count = new_count
                time.sleep(1)

            # Update cursor: Discord returns messages newest first, so use
            # the ID of the last (oldest) message in this page as the 'before' cursor
            if messages:
                before = messages[-1].get('id')

            if should_stop:
                break

        # Sync active threads separately — thread replies can be added to old
        # messages that the channel message pagination may never reach during
        # incremental sync. This matches Onyx's approach: enumerate threads
        # directly, then use time-filtered history for each thread.
        try:
            active_threads = discord_api.fetch_forum_threads(guild_id, channel_id)
            for thr in active_threads:
                thread_id = thr.get('id')
                if not thread_id or thread_id in synced_thread_ids:
                    continue
                # Only sync threads with activity after last_sync_time
                last_msg_id = thr.get('last_message_id')
                if last_sync_time and last_msg_id:
                    last_activity_ms = (int(last_msg_id) >> 22) + 1420070400000
                    last_activity_time = datetime.datetime.fromtimestamp(
                        last_activity_ms / 1000, tz=datetime.timezone.utc
                    )
                    if last_activity_time <= last_sync_time:
                        continue
                # Try to find parent message by thread_id first
                parent = discord_seadb_api.get_message_by_thread_id(connection_id, thread_id)

                if parent:
                    # Parent exists — only fetch new replies since last sync
                    thread_messages = self._fetch_all_messages(
                        discord_api, thread_id, last_sync_time=last_sync_time
                    )
                    if thread_messages:
                        reply_result = discord_seadb_api.update_message_replies(
                            connection_id, parent.get('message_id'), thread_messages
                        )
                        if reply_result['added'] + reply_result['updated'] + reply_result['deleted'] > 0:
                            sync_count += 1
                else:
                    # Parent was never synced — fetch all messages to create it
                    thread_messages = self._fetch_all_messages(discord_api, thread_id)
                    if not thread_messages:
                        continue
                    first_msg = thread_messages[-1]
                    parent_message_id = first_msg.get('id')
                    msg_data = dict(first_msg)
                    msg_data['thread_id'] = thread_id
                    if thr.get('name'):
                        msg_data['title'] = thr['name']
                    discord_seadb_api.insert_message(connection_id, msg_data)
                    sync_count += 1

                    reply_messages = [m for m in thread_messages if m.get('id') != parent_message_id]
                    if reply_messages:
                        reply_result = discord_seadb_api.update_message_replies(
                            connection_id, parent_message_id, reply_messages
                        )
                        if reply_result['added'] + reply_result['updated'] + reply_result['deleted'] > 0:
                            sync_count += 1
        except Exception as e:
            logger.warning('Failed to sync active threads for channel %s: %s', channel_id, e)

        connection_status['last_sync_changes'] = {
            'added': added_count,
            'deleted': 0,
            'updated': updated_count,
        }
        total_count = discord_seadb_api.get_message_count(connection_id)
        return sync_count, total_count

    @staticmethod
    def _fetch_all_messages(discord_api, channel_id, last_sync_time=None):
        """Fetch all messages from a channel/thread with pagination.

        Discord returns messages newest-first. Paginates until exhaustion
        or until messages older than last_sync_time are encountered.

        Args:
            discord_api: DiscordAPI instance.
            channel_id: Channel or thread ID to fetch messages from.
            last_sync_time: Optional datetime; stop when messages are
                older than or equal to this time.

        Returns:
            list[dict]: All fetched message objects (newest first).
        """
        all_messages = []
        before = None

        while True:
            messages = discord_api.fetch_channel_messages(channel_id, before=before)
            if not messages:
                break

            for msg in messages:
                if last_sync_time:
                    msg_timestamp = msg.get('edited_timestamp') or msg.get('timestamp')
                    if msg_timestamp:
                        msg_time = time_to_utc_time(msg_timestamp)
                        if msg_time <= last_sync_time:
                            return all_messages
                all_messages.append(msg)

            if len(messages) < 100:
                break

            before = messages[-1].get('id')

        return all_messages

    def _sync_discord_forum(self, connection_id, channel_id, guild_id, last_sync_time,
                            connection_status, discord_api, discord_seadb_api, seaqa_db):
        """Sync a forum channel: each thread is a record, thread replies are reply rows."""
        sync_count = 0
        added_count = 0
        updated_count = 0
        _status_last_update_time = datetime.datetime.now()
        _status_last_update_count = 0
        should_stop = False

        if last_sync_time:
            last_sync_time = time_to_utc_time(str(last_sync_time))

        while True:
            threads = discord_api.fetch_forum_threads(guild_id, channel_id)

            if not threads:
                break

            for thread in threads:
                thread_id = thread.get('id')
                thread_name = thread.get('name', '')
                thread_metadata = thread.get('thread_metadata', {})

                # Use last_message_id to determine last activity time
                # Discord snowflake ID encodes the creation timestamp
                last_message_id = thread.get('last_message_id')
                if last_message_id:
                    last_msg_ms = (int(last_message_id) >> 22) + 1420070400000
                    last_activity_time = datetime.datetime.fromtimestamp(
                        last_msg_ms / 1000, tz=datetime.timezone.utc
                    )
                else:
                    thread_created = thread_metadata.get('create_timestamp')
                    if thread_created:
                        last_activity_time = time_to_utc_time(thread_created)
                    else:
                        continue

                if last_sync_time and last_activity_time <= last_sync_time:
                    should_stop = True
                    break

                # Fetch all messages in the thread (no last_sync_time limit —
                # we need the opening post at thread_messages[-1])
                thread_messages = self._fetch_all_messages(discord_api, thread_id)
                if not thread_messages:
                    continue

                # The first message (oldest, last in the returned list since
                # Discord returns newest-first) is the thread's opening post
                first_msg = thread_messages[-1] if thread_messages else None
                if not first_msg:
                    continue

                first_author = first_msg.get('author', {}).get('username', 'Unknown')
                first_content = first_msg.get('content', '') or ''
                first_timestamp = first_msg.get('edited_timestamp') or first_msg.get('timestamp')

                # Build a record from the thread + first message
                record_data = {
                    'id': thread_id,
                    'title': thread_name,
                    'author': first_author,
                    'content': first_content,
                    'timestamp': first_timestamp,
                    'edited_timestamp': first_timestamp,
                }

                # Check if this thread record already exists
                existing_message = discord_seadb_api.get_message_by_message_id(connection_id, thread_id)
                record_changed = True
                if existing_message:
                    existing_modified_time = time_to_utc_time(str(existing_message.get('modified_time')))
                    first_message_time = time_to_utc_time(first_timestamp) if first_timestamp else None
                    if first_message_time and first_message_time <= existing_modified_time:
                        record_changed = False

                if record_changed:
                    event_type = (
                        DataEventType.DISCORD_MESSAGE_UPDATED.value
                        if existing_message
                        else DataEventType.DISCORD_MESSAGE_ADDED.value
                    )
                    primary_key = existing_message.get('_pk')
                    if primary_key:
                        discord_seadb_api.update_message(connection_id, primary_key, record_data)
                        updated_count += 1
                    else:
                        discord_seadb_api.insert_message(connection_id, record_data)
                        added_count += 1

                    sync_count += 1

                    self.publish_synced_record_events(
                        get_latest_row=lambda: discord_seadb_api.get_message_by_message_id(connection_id, thread_id),
                        project_uuid=discord_seadb_api.base_id,
                        connection_id=connection_id,
                        item_id=thread_id,
                        old_row=existing_message,
                        state_event_type=event_type,
                        state_event_ignored_fields=('modified_time', 'record_modified_time', 'sync_time'),
                        log_object_name='forum thread',
                    )

                # Always sync thread replies (all messages except the first one)
                # — they can change even when the opening post hasn't been edited
                reply_messages = thread_messages[:-1] if len(thread_messages) > 1 else []
                if reply_messages:
                    reply_result = discord_seadb_api.update_message_replies(
                        connection_id, thread_id, reply_messages
                    )
                    if reply_result['added'] + reply_result['updated'] + reply_result['deleted'] > 0 and not record_changed:
                        sync_count += 1

                should, new_time, new_count = need_update_connection_last_sync_count(
                    sync_count, _status_last_update_time, _status_last_update_count
                )
                if should:
                    connection_status['last_sync_count'] = sync_count
                    seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
                    _status_last_update_time = new_time
                    _status_last_update_count = new_count

                time.sleep(1)

            if should_stop:
                break

            break

        connection_status['last_sync_changes'] = {
            'added': added_count,
            'deleted': 0,
            'updated': updated_count,
        }
        total_count = discord_seadb_api.get_message_count(connection_id)
        return sync_count, total_count
