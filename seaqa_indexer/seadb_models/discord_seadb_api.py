import logging
import datetime

from seaqa_indexer.utils.seadb_api import SeaDBAPI, TableNotFound
from seaqa_indexer.utils import time_to_utc_time
from seaqa_indexer.seadb_models.models import SchemaTables


logger = logging.getLogger(__name__)

MAX_CONTENT_LENGTH = 10000
DISCORD_EPOCH_MS = 1420070400000


class DiscordSeaDBAPI:
    """SeaDB CRUD operations for Discord threads and messages."""

    def __init__(self, base_id, timeout=30):
        self.base_id = base_id
        self.seadb_api = SeaDBAPI(timeout=timeout)

    # ---- Helpers ----

    @staticmethod
    def _snowflake_to_datetime(snowflake):
        """Convert a Discord snowflake ID string to a UTC datetime."""
        if not snowflake:
            return None
        timestamp_ms = (int(snowflake) >> 22) + DISCORD_EPOCH_MS
        return datetime.datetime.fromtimestamp(timestamp_ms / 1000, tz=datetime.UTC)

    # ---- Threads ----

    def save_or_update_thread_to_db(self, connection_id, thread, existing_thread=None):
        """Insert or update a thread record (metadata only, no message content)."""
        thread_id = thread.get('id')
        thread_name = thread.get('name', '')
        title = thread_name
        modified_time = self._snowflake_to_datetime(thread.get('last_message_id'))
        now_datetime = datetime.datetime.now(datetime.UTC).isoformat()

        row = {
            SchemaTables.DISCORD_THREADS.column.thread_id.name: str(thread_id),
            SchemaTables.DISCORD_THREADS.column.title.name: title,
            SchemaTables.DISCORD_THREADS.column.modified_time.name: modified_time.isoformat(),
            SchemaTables.DISCORD_THREADS.column.sync_time.name: now_datetime,
            SchemaTables.DISCORD_THREADS.column.record_modified_time.name: now_datetime,
            SchemaTables.DISCORD_THREADS.column.deleted.name: False,
        }

        table_name = SchemaTables.DISCORD_THREADS.table_name(connection_id=connection_id)

        if existing_thread:
            self.seadb_api.update_rows(self.base_id, table_name, [{
                "pk": existing_thread.get('_pk'),
                "row": row,
            }])
        else:
            created_time = time_to_utc_time(
                thread.get('thread_metadata', {}).get('create_timestamp')
            )
            row[SchemaTables.DISCORD_THREADS.column.created_time.name] = created_time.isoformat()
            self.seadb_api.insert_rows(self.base_id, table_name, [row])
        return True

    def touch_thread(self, connection_id, thread_id, last_message_id=None):
        """Mark a thread row as changed so index/AI processors can refresh derived data."""
        table_name = SchemaTables.DISCORD_THREADS.table_name(connection_id=connection_id)
        now = datetime.datetime.now(datetime.UTC).isoformat()
        row = {
            SchemaTables.DISCORD_THREADS.column.sync_time.name: now,
            SchemaTables.DISCORD_THREADS.column.record_modified_time.name: now,
        }
        modified_time = self._snowflake_to_datetime(last_message_id)
        if modified_time:
            row[SchemaTables.DISCORD_THREADS.column.modified_time.name] = modified_time.isoformat()

        thread = self.get_thread_by_thread_id(connection_id, thread_id)
        if not thread:
            logger.debug(f"Thread {thread_id} not found in connection {connection_id}, skipping touch")
            return None

        return self.seadb_api.update_rows(self.base_id, table_name, [{
            "pk": thread.get('_pk'),
            "row": row,
        }])

    def get_thread_by_thread_id(self, connection_id, thread_id):
        table_name = SchemaTables.DISCORD_THREADS.table_name(connection_id=connection_id)
        sql = f"""SELECT `_pk`, `thread_id`, `title`,
                `created_time`, `modified_time`, `deleted`, `outdated`,
                `record_modified_time`, `sync_time`, `ai_processed_time`
                FROM `{table_name}` WHERE `thread_id` = '{thread_id}'"""
        response = self.seadb_api.query_rows(self.base_id, sql)
        if response and 'results' in response and response['results']:
            return response['results'][0]
        return {}

    def delete_thread(self, connection_id, thread_id):
        """Logically delete a thread record."""
        table_name = SchemaTables.DISCORD_THREADS.table_name(connection_id=connection_id)
        thread = self.get_thread_by_thread_id(connection_id, thread_id)
        if not thread:
            logger.debug(f"Thread {thread_id} not found in connection {connection_id}, skipping deletion")
            return None
        primary_key = thread.get('_pk')
        updates = [{
            "pk": primary_key,
            "row": {
                "deleted": True,
                "sync_time": datetime.datetime.now(datetime.UTC).isoformat(),
                "record_modified_time": datetime.datetime.now(datetime.UTC).isoformat()
            }
        }]
        return self.seadb_api.update_rows(self.base_id, table_name, updates)

    def get_thread_count(self, connection_id):
        table_name = SchemaTables.DISCORD_THREADS.table_name(connection_id=connection_id)
        sql = f"SELECT COUNT(*) as count FROM `{table_name}`"
        result = self.seadb_api.query_rows(self.base_id, sql)
        count = 0
        if result and 'results' in result and result['results']:
            count = result['results'][0].get('count', 0)
        return count

    def get_thread_ids_by_page(self, connection_id, page_size, offset):
        """Get thread IDs for pagination (used by cleanup to compare local vs remote)."""
        table_name = SchemaTables.DISCORD_THREADS.table_name(connection_id=connection_id)
        sql = f"""SELECT `thread_id` FROM `{table_name}`
                WHERE deleted = false OR deleted IS NULL
                LIMIT {page_size} OFFSET {offset}"""
        response = self.seadb_api.query_rows(self.base_id, sql)
        return response.get('results', [])

    def mark_thread_deleted(self, connection_id, thread_id):
        """Soft-delete a thread by thread_id without needing _pk lookup."""
        table_name = SchemaTables.DISCORD_THREADS.table_name(connection_id=connection_id)
        sql = f"""UPDATE `{table_name}` SET deleted = true,
                sync_time = '{datetime.datetime.now(datetime.UTC).isoformat()}',
                record_modified_time = '{datetime.datetime.now(datetime.UTC).isoformat()}'
                WHERE thread_id = '{thread_id}' AND (deleted = false OR deleted IS NULL)"""
        return self.seadb_api.query_rows(self.base_id, sql)

    # ---- Messages ----

    @staticmethod
    def _message_to_comparable_row(message):
        content = message.get('content', '') or ''
        if content and len(content) > MAX_CONTENT_LENGTH:
            content = content[:MAX_CONTENT_LENGTH]
        modified_time = message.get('edited_timestamp') or message.get('timestamp')
        return {
            'author': message.get('author', {}).get('username', ''),
            'content': content,
            'modified_time': time_to_utc_time(modified_time).isoformat(),
        }

    @classmethod
    def _message_has_changed(cls, existing_message, message):
        new_message = cls._message_to_comparable_row(message)
        existing_modified_time = existing_message.get('modified_time')
        if existing_modified_time:
            existing_modified_time = time_to_utc_time(str(existing_modified_time)).isoformat()

        return (
            (existing_message.get('author') or '') != new_message['author']
            or (existing_message.get('content') or '') != new_message['content']
            or existing_modified_time != new_message['modified_time']
        )

    def add_or_update_messages(self, connection_id, thread_id, messages):
        """Insert or update messages for a thread.

        All messages (including the opening post) are stored here.
        Deletion of stale messages is handled by the cleaner task.
        """
        table_name = SchemaTables.DISCORD_THREAD_MESSAGES.table_name(connection_id=connection_id)
        offset = 0
        page_size = 1000
        message_id_to_pk = {}
        added_messages = []

        # Collect existing message PKs for change detection
        while True:
            existing = self.get_thread_messages_by_page(connection_id, thread_id, page_size, offset)
            for msg in existing:
                mid = msg.get('message_id')
                message_id_to_pk[mid] = msg
            if not existing or len(existing) < page_size:
                break
            offset += page_size

        message_updated_count = 0
        for message in messages:
            message_type = message.get('type', 0)
            if message_type not in (0, 19, 21):
                continue
            mid = message.get('id')

            author = message.get('author', {}).get('username', '')
            content = message.get('content', '') or ''
            created_time = message.get('timestamp')
            edited_timestamp = message.get('edited_timestamp')
            modified_time = edited_timestamp if edited_timestamp else created_time

            if content and len(content) > MAX_CONTENT_LENGTH:
                content = content[:MAX_CONTENT_LENGTH]

            insert_row = {
                SchemaTables.DISCORD_THREAD_MESSAGES.column.thread_id.name: str(thread_id),
                SchemaTables.DISCORD_THREAD_MESSAGES.column.message_id.name: str(mid),
                SchemaTables.DISCORD_THREAD_MESSAGES.column.author.name: author,
                SchemaTables.DISCORD_THREAD_MESSAGES.column.content.name: content,
                SchemaTables.DISCORD_THREAD_MESSAGES.column.modified_time.name: time_to_utc_time(modified_time).isoformat(),
            }

            existing_message = message_id_to_pk.get(mid)
            if existing_message:
                if self._message_has_changed(existing_message, message):
                    message_updated_count += 1
                    self.seadb_api.update_rows(self.base_id, table_name, [{
                        "pk": existing_message.get('_pk'),
                        "row": insert_row,
                    }])
            else:
                insert_row[SchemaTables.DISCORD_THREAD_MESSAGES.column.created_time.name] = time_to_utc_time(created_time).isoformat()
                self.seadb_api.insert_rows(self.base_id, table_name, [insert_row])
                added_messages.append({
                    'message_id': mid,
                    'author': author,
                    'content': content,
                    'modified_time': time_to_utc_time(modified_time).isoformat(),
                })

        return {
            'added': len(added_messages),
            'updated': message_updated_count,
            'deleted': 0,
            'added_messages': added_messages,
        }

    def get_thread_first_message(self, connection_id, thread_id):
        """Get the first message (opening post) for a thread."""
        table_name = SchemaTables.DISCORD_THREAD_MESSAGES.table_name(connection_id=connection_id)
        if not thread_id:
            return {}
        sql = f"""SELECT content FROM `{table_name}`
                WHERE thread_id = '{thread_id}' ORDER BY created_time ASC LIMIT 1"""
        result = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
        if result:
            return result[0]
        return {}

    def get_thread_messages_by_page(self, connection_id, thread_id, page_size, offset):
        table_name = SchemaTables.DISCORD_THREAD_MESSAGES.table_name(connection_id=connection_id)
        sql = f"""SELECT `_pk`, `message_id`, `author`, `content`, `modified_time` FROM `{table_name}`
                WHERE thread_id = '{thread_id}' LIMIT {page_size} OFFSET {offset}"""
        messages = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
        return messages

    def delete_message_from_db(self, connection_id, thread_id, message_id):
        table_name = SchemaTables.DISCORD_THREAD_MESSAGES.table_name(connection_id=connection_id)
        sql = f"DELETE FROM `{table_name}` WHERE thread_id = '{thread_id}' AND message_id = '{message_id}'"
        return self.seadb_api.query_rows(self.base_id, sql)

    # ---- Index / AI helpers ----

    def get_threads_by_page(self, connection_id, since, page_size, offset):
        table_name = SchemaTables.DISCORD_THREADS.table_name(connection_id=connection_id)
        sql = f"""SELECT `_pk`, `thread_id`, `deleted`, `outdated`, `title`, `modified_time`
                FROM `{table_name}`
                WHERE record_modified_time >= '{since}'
                LIMIT {page_size} OFFSET {offset}"""
        response = self.seadb_api.query_rows(self.base_id, sql)
        return response.get('results', [])

    def get_thread_messages_for_index(self, connection_id, thread_id):
        """Get all messages content for a thread (used during search indexing)."""
        table_name = SchemaTables.DISCORD_THREAD_MESSAGES.table_name(connection_id=connection_id)
        offset = 0
        page_size = 1000
        all_content = ''
        while True:
            sql = f"""SELECT `content` FROM `{table_name}`
                    WHERE thread_id = '{thread_id}' LIMIT {page_size} OFFSET {offset}"""
            results = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
            if not results:
                break
            for row in results:
                all_content += (row.get('content') or '')
            if len(results) < page_size:
                break
            offset += page_size
        return all_content

    def get_need_ai_threads(self, connection_id, page_size):
        table_name = SchemaTables.DISCORD_THREADS.table_name(connection_id=connection_id)
        sql = f"""SELECT _pk, thread_id, title FROM `{table_name}`
        WHERE (ai_processed_time IS NULL OR ai_processed_time < record_modified_time) AND (deleted IS NULL OR deleted = false)
        LIMIT {page_size}"""
        result = self.seadb_api.query_rows(self.base_id, sql)
        return result.get('results', [])
