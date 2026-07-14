import logging
import datetime

from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.utils import time_to_utc_time
from seaqa_indexer.seadb_models.models import SchemaTables


logger = logging.getLogger(__name__)

MAX_CONTENT_LENGTH = 10000


class DiscordSeaDBAPI:
    """SeaDB CRUD operations for Discord messages and replies."""

    def __init__(self, base_id, timeout=30):
        self.base_id = base_id
        self.seadb_api = SeaDBAPI(timeout=timeout)

    # ---- Messages ----

    @staticmethod
    def _extract_author(message_data):
        author = message_data.get('author', 'Unknown')
        if isinstance(author, dict):
            return author.get('username', 'Unknown')
        return str(author) if author else 'Unknown'

    @staticmethod
    def _build_title(message_data, author):
        # Use explicit title if provided (forum threads), otherwise auto-generate
        explicit = message_data.get('title')
        if explicit:
            return explicit
        content = message_data.get('content', '') or ''
        return content[:200] if content else f'Message from {author}'

    def insert_message(self, connection_id, message_data):
        table_name = SchemaTables.DISCORD_MESSAGES.table_name(connection_id=connection_id)

        message_id = message_data.get('id')
        thread_id = message_data.get('thread_id')
        author = self._extract_author(message_data)
        content = message_data.get('content', '') or ''
        created_time = message_data.get('timestamp')
        edited_timestamp = message_data.get('edited_timestamp')
        modified_time = edited_timestamp if edited_timestamp else created_time
        title = self._build_title(message_data, author)

        if content and len(content) > MAX_CONTENT_LENGTH:
            content = content[:MAX_CONTENT_LENGTH]

        rows = [{
            SchemaTables.DISCORD_MESSAGES.column.message_id.name: str(message_id),
            SchemaTables.DISCORD_MESSAGES.column.thread_id.name: str(thread_id) if thread_id else '',
            SchemaTables.DISCORD_MESSAGES.column.title.name: title,
            SchemaTables.DISCORD_MESSAGES.column.author.name: author,
            SchemaTables.DISCORD_MESSAGES.column.content.name: content,
            SchemaTables.DISCORD_MESSAGES.column.created_time.name: time_to_utc_time(created_time).isoformat(),
            SchemaTables.DISCORD_MESSAGES.column.modified_time.name: time_to_utc_time(modified_time).isoformat(),
            SchemaTables.DISCORD_MESSAGES.column.sync_time.name: datetime.datetime.now(datetime.UTC).isoformat(),
            SchemaTables.DISCORD_MESSAGES.column.deleted.name: False,
            SchemaTables.DISCORD_MESSAGES.column.record_modified_time.name: datetime.datetime.now(datetime.UTC).isoformat(),
        }]
        return self.seadb_api.insert_rows(self.base_id, table_name, rows)

    def update_message(self, connection_id, primary_key, message_data):
        table_name = SchemaTables.DISCORD_MESSAGES.table_name(connection_id=connection_id)

        author = self._extract_author(message_data)
        content = message_data.get('content', '') or ''
        edited_timestamp = message_data.get('edited_timestamp')
        created_time = message_data.get('timestamp')
        modified_time = edited_timestamp if edited_timestamp else created_time
        title = self._build_title(message_data, author)

        if content and len(content) > MAX_CONTENT_LENGTH:
            content = content[:MAX_CONTENT_LENGTH]

        updates = [{
            "pk": primary_key,
            "row": {
                SchemaTables.DISCORD_MESSAGES.column.title.name: title,
                SchemaTables.DISCORD_MESSAGES.column.author.name: author,
                SchemaTables.DISCORD_MESSAGES.column.content.name: content,
                SchemaTables.DISCORD_MESSAGES.column.modified_time.name: time_to_utc_time(modified_time).isoformat(),
                SchemaTables.DISCORD_MESSAGES.column.sync_time.name: datetime.datetime.now(datetime.UTC).isoformat(),
                SchemaTables.DISCORD_MESSAGES.column.record_modified_time.name: datetime.datetime.now(datetime.UTC).isoformat(),
                SchemaTables.DISCORD_MESSAGES.column.deleted.name: False,
            }
        }]
        return self.seadb_api.update_rows(self.base_id, table_name, updates)

    def get_message_by_message_id(self, connection_id, message_id):
        table_name = SchemaTables.DISCORD_MESSAGES.table_name(connection_id=connection_id)
        sql = f"""SELECT `_pk`, `message_id`, `thread_id`, `author`, `content`,
                `created_time`, `modified_time`, `deleted`, `outdated`,
                `record_modified_time`, `sync_time`, `ai_processed_time`
                FROM `{table_name}` WHERE `message_id` = '{message_id}'"""
        response = self.seadb_api.query_rows(self.base_id, sql)
        if response and 'results' in response and response['results']:
            return response['results'][0]
        return {}

    def get_message_by_thread_id(self, connection_id, thread_id):
        """Find a parent message by its thread_id."""
        table_name = SchemaTables.DISCORD_MESSAGES.table_name(connection_id=connection_id)
        sql = f"""SELECT `_pk`, `message_id`, `thread_id`, `author`, `content`,
                `created_time`, `modified_time`, `deleted`, `outdated`,
                `record_modified_time`, `sync_time`
                FROM `{table_name}` WHERE `thread_id` = '{thread_id}'"""
        response = self.seadb_api.query_rows(self.base_id, sql)
        if response and 'results' in response and response['results']:
            return response['results'][0]
        return {}

    def delete_message(self, connection_id, message_id):
        """Logically delete a message record."""
        table_name = SchemaTables.DISCORD_MESSAGES.table_name(connection_id=connection_id)
        message = self.get_message_by_message_id(connection_id, message_id)
        if not message:
            logger.debug(f"Message {message_id} not found in connection {connection_id}, skipping deletion")
            return None
        primary_key = message.get('_pk')
        updates = [{
            "pk": primary_key,
            "row": {
                "deleted": True,
                "sync_time": datetime.datetime.now(datetime.UTC).isoformat(),
                "record_modified_time": datetime.datetime.now(datetime.UTC).isoformat()
            }
        }]
        return self.seadb_api.update_rows(self.base_id, table_name, updates)

    def get_message_count(self, connection_id):
        table_name = SchemaTables.DISCORD_MESSAGES.table_name(connection_id=connection_id)
        sql = f"SELECT COUNT(*) as count FROM `{table_name}`"
        result = self.seadb_api.query_rows(self.base_id, sql)
        count = 0
        if result and 'results' in result and result['results']:
            count = result['results'][0].get('count', 0)
        return count

    def get_message_ids_by_page(self, connection_id, page_size, offset):
        """Get message IDs for pagination (used by cleanup to compare local vs remote)."""
        table_name = SchemaTables.DISCORD_MESSAGES.table_name(connection_id=connection_id)
        sql = f"""SELECT `message_id` FROM `{table_name}`
                WHERE deleted = false OR deleted IS NULL
                LIMIT {page_size} OFFSET {offset}"""
        response = self.seadb_api.query_rows(self.base_id, sql)
        return response.get('results', [])

    def mark_message_deleted(self, connection_id, message_id):
        """Soft-delete a message by message_id without needing _pk lookup."""
        table_name = SchemaTables.DISCORD_MESSAGES.table_name(connection_id=connection_id)
        sql = f"""UPDATE `{table_name}` SET deleted = true,
                sync_time = '{datetime.datetime.now(datetime.UTC).isoformat()}',
                record_modified_time = '{datetime.datetime.now(datetime.UTC).isoformat()}'
                WHERE message_id = '{message_id}' AND (deleted = false OR deleted IS NULL)"""
        return self.seadb_api.query_rows(self.base_id, sql)

    # ---- Replies (thread messages) ----

    def insert_reply(self, connection_id, message_id, reply_data):
        table_name = SchemaTables.DISCORD_MESSAGE_REPLIES.table_name(connection_id=connection_id)

        reply_id = reply_data.get('id')
        author = reply_data.get('author', {}).get('username', 'Unknown')
        content = reply_data.get('content', '') or ''
        created_time = reply_data.get('timestamp')
        edited_timestamp = reply_data.get('edited_timestamp')
        modified_time = edited_timestamp if edited_timestamp else created_time

        if content and len(content) > MAX_CONTENT_LENGTH:
            content = content[:MAX_CONTENT_LENGTH]

        rows = [{
            SchemaTables.DISCORD_MESSAGE_REPLIES.column.message_id.name: str(message_id),
            SchemaTables.DISCORD_MESSAGE_REPLIES.column.reply_id.name: str(reply_id),
            SchemaTables.DISCORD_MESSAGE_REPLIES.column.author.name: author,
            SchemaTables.DISCORD_MESSAGE_REPLIES.column.content.name: content,
            SchemaTables.DISCORD_MESSAGE_REPLIES.column.created_time.name: time_to_utc_time(created_time).isoformat(),
            SchemaTables.DISCORD_MESSAGE_REPLIES.column.modified_time.name: time_to_utc_time(modified_time).isoformat(),
        }]
        return self.seadb_api.insert_rows(self.base_id, table_name, rows)

    def update_reply(self, connection_id, reply_pk, message_id, reply_data):
        table_name = SchemaTables.DISCORD_MESSAGE_REPLIES.table_name(connection_id=connection_id)

        reply_id = reply_data.get('id')
        author = reply_data.get('author', {}).get('username', 'Unknown')
        content = reply_data.get('content', '') or ''
        edited_timestamp = reply_data.get('edited_timestamp')
        created_time = reply_data.get('timestamp')
        modified_time = edited_timestamp if edited_timestamp else created_time

        if content and len(content) > MAX_CONTENT_LENGTH:
            content = content[:MAX_CONTENT_LENGTH]

        updates = [{
            "pk": reply_pk,
            "row": {
                SchemaTables.DISCORD_MESSAGE_REPLIES.column.author.name: author,
                SchemaTables.DISCORD_MESSAGE_REPLIES.column.content.name: content,
                SchemaTables.DISCORD_MESSAGE_REPLIES.column.message_id.name: str(message_id),
                SchemaTables.DISCORD_MESSAGE_REPLIES.column.reply_id.name: str(reply_id),
                SchemaTables.DISCORD_MESSAGE_REPLIES.column.modified_time.name: time_to_utc_time(modified_time).isoformat(),
            }
        }]
        return self.seadb_api.update_rows(self.base_id, table_name, updates)

    def update_message_replies(self, connection_id, message_id, replies):
        """Sync replies for a message using diff-based approach.

        Compares existing replies in SeaDB with new replies from Discord,
        inserts/updates new ones, and deletes removed ones.
        """
        offset = 0
        page_size = 1000
        old_reply_ids = set()
        new_reply_ids = set()
        reply_id_to_pk = {}
        added_replies = []

        # Collect existing reply IDs
        while True:
            existing = self.get_message_replies_by_page(connection_id, message_id, page_size, offset)
            for reply in existing:
                rid = reply.get('reply_id')
                reply_id_to_pk[rid] = reply.get('_pk')
                old_reply_ids.add(rid)
            if not existing or len(existing) < page_size:
                break
            offset += page_size

        # Insert or update each incoming reply (skip non-DEFAULT messages,
        # e.g. type 21 THREAD_STARTER_MESSAGE which Discord's UI hides)
        reply_updated_count = 0
        for reply in replies:
            reply_type = reply.get('type', 0)
            if reply_type not in (0, 19):
                continue
            rid = reply.get('id')
            new_reply_ids.add(rid)

            pk = reply_id_to_pk.get(rid)
            if pk:
                self.update_reply(connection_id, pk, message_id, reply)
                reply_updated_count += 1
            else:
                self.insert_reply(connection_id, message_id, reply)
                added_replies.append({
                    'reply_id': rid,
                    'author': reply.get('author', {}).get('username', 'Unknown'),
                    'content': (reply.get('content') or '')[:MAX_CONTENT_LENGTH],
                    'modified_time': time_to_utc_time(
                        reply.get('edited_timestamp') or reply.get('timestamp')
                    ).isoformat(),
                })

        # Delete removed replies
        need_deleted_reply_ids = old_reply_ids - new_reply_ids
        for rid in need_deleted_reply_ids:
            self.delete_reply_from_db(connection_id, message_id, rid)

        return {
            'added': len(added_replies),
            'updated': reply_updated_count,
            'deleted': len(need_deleted_reply_ids),
            'added_replies': added_replies,
        }

    def get_message_replies_by_page(self, connection_id, message_id, page_size, offset):
        table_name = SchemaTables.DISCORD_MESSAGE_REPLIES.table_name(connection_id=connection_id)
        sql = f"""SELECT `_pk`, `reply_id`, `content` FROM `{table_name}`
                WHERE message_id = '{message_id}' LIMIT {page_size} OFFSET {offset}"""
        replies = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
        return replies

    def delete_reply_from_db(self, connection_id, message_id, reply_id):
        table_name = SchemaTables.DISCORD_MESSAGE_REPLIES.table_name(connection_id=connection_id)
        sql = f"DELETE FROM `{table_name}` WHERE message_id = '{message_id}' AND reply_id = '{reply_id}'"
        return self.seadb_api.query_rows(self.base_id, sql)

    def get_reply_by_reply_id(self, connection_id, message_id, reply_id):
        """Get a single reply by its reply_id under a parent message."""
        table_name = SchemaTables.DISCORD_MESSAGE_REPLIES.table_name(connection_id=connection_id)
        sql = f"""SELECT `_pk`, `reply_id` FROM `{table_name}`
                WHERE message_id = '{message_id}' AND reply_id = '{reply_id}'"""
        response = self.seadb_api.query_rows(self.base_id, sql)
        if response and 'results' in response and response['results']:
            return response['results'][0]
        return {}

    def upsert_inline_reply(self, connection_id, message_id, reply_data):
        """Insert or update a single inline reply (type 19) by reply_id.

        Unlike update_message_replies which does a full diff-replace for
        thread messages, this handles one reply at a time without deleting
        other replies for the same parent.
        """
        reply_id = reply_data.get('id')
        existing = self.get_reply_by_reply_id(connection_id, message_id, reply_id)
        if existing and existing.get('_pk'):
            return self.update_reply(connection_id, existing['_pk'], message_id, reply_data)
        else:
            return self.insert_reply(connection_id, message_id, reply_data)

    def get_messages_by_page(self, connection_id, since, page_size, offset):
        table_name = SchemaTables.DISCORD_MESSAGES.table_name(connection_id=connection_id)
        sql = f"""SELECT `_pk`, `message_id`, `deleted`, `outdated`, `title`, `content`, `modified_time`
                FROM `{table_name}`
                WHERE record_modified_time >= '{since}'
                LIMIT {page_size} OFFSET {offset}"""
        response = self.seadb_api.query_rows(self.base_id, sql)
        return response.get('results', [])

    def get_message_replies_for_index(self, connection_id, message_id):
        """Get all replies for a message (used during search indexing)."""
        table_name = SchemaTables.DISCORD_MESSAGE_REPLIES.table_name(connection_id=connection_id)
        offset = 0
        page_size = 1000
        all_content = ''
        while True:
            sql = f"""SELECT `content` FROM `{table_name}`
                    WHERE message_id = '{message_id}' LIMIT {page_size} OFFSET {offset}"""
            results = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
            if not results:
                break
            for row in results:
                all_content += (row.get('content') or '')
            if len(results) < page_size:
                break
            offset += page_size
        return all_content

    def get_need_ai_messages(self, connection_id, page_size):
        table_name = SchemaTables.DISCORD_MESSAGES.table_name(connection_id=connection_id)
        sql = f"""SELECT _pk, message_id, title, content FROM `{table_name}`
        WHERE (ai_processed_time IS NULL OR ai_processed_time < record_modified_time) AND (deleted IS NULL OR deleted = false)
        LIMIT {page_size}"""
        result = self.seadb_api.query_rows(self.base_id, sql)
        return result.get('results', [])
