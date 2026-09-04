import logging
import datetime

from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.utils import time_to_utc_time
from seaqa_indexer.seadb_models.models import SchemaTables


logger = logging.getLogger(__name__)

MAX_CONTENT_LENGTH = 10000


class SlackSeaDBAPI:
    """SeaDB CRUD operations for Slack channel messages and thread replies."""

    def __init__(self, base_id, timeout=30):
        self.base_id = base_id
        self.seadb_api = SeaDBAPI(timeout=timeout)

    # ---- Helpers ----

    @staticmethod
    def ts_to_datetime(ts):
        """Convert a Slack ``ts`` (unix seconds.microseconds) to a UTC datetime."""
        if not ts:
            return None
        try:
            return datetime.datetime.fromtimestamp(float(ts), tz=datetime.UTC)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def make_title(text):
        """Slack messages have no title; derive one from the message text."""
        text = (text or '').replace('\r', ' ').replace('\n', ' ')
        return ' '.join(text.split())[:120]

    @staticmethod
    def make_content(text):
        text = text or ''
        if len(text) > MAX_CONTENT_LENGTH:
            return text[:MAX_CONTENT_LENGTH]
        return text

    # ---- Messages (main table) ----

    def save_or_update_message(self, connection_id, channel_id, message, author, existing_message=None):
        """Insert or update a top-level message record."""
        ts = message.get('ts')
        text = message.get('text', '') or ''
        created_time = self.ts_to_datetime(ts)
        edited_ts = (message.get('edited') or {}).get('ts')
        modified_time = self.ts_to_datetime(edited_ts) or created_time
        now = datetime.datetime.now(datetime.UTC).isoformat()

        row = {
            SchemaTables.SLACK_MESSAGES.column.message_id.name: str(ts),
            SchemaTables.SLACK_MESSAGES.column.channel_id.name: str(channel_id),
            SchemaTables.SLACK_MESSAGES.column.title.name: self.make_title(text),
            SchemaTables.SLACK_MESSAGES.column.author.name: author,
            SchemaTables.SLACK_MESSAGES.column.content.name: self.make_content(text),
            SchemaTables.SLACK_MESSAGES.column.reply_count.name: int(message.get('reply_count', 0) or 0),
            SchemaTables.SLACK_MESSAGES.column.latest_reply.name: message.get('latest_reply', '') or '',
            SchemaTables.SLACK_MESSAGES.column.modified_time.name: modified_time.isoformat(),
            SchemaTables.SLACK_MESSAGES.column.sync_time.name: now,
            SchemaTables.SLACK_MESSAGES.column.record_modified_time.name: now,
            SchemaTables.SLACK_MESSAGES.column.deleted.name: False,
        }

        table_name = SchemaTables.SLACK_MESSAGES.table_name(connection_id=connection_id)

        if existing_message:
            self.seadb_api.update_rows(self.base_id, table_name, [{
                "pk": existing_message.get('_pk'),
                "row": row,
            }])
        else:
            row[SchemaTables.SLACK_MESSAGES.column.created_time.name] = created_time.isoformat()
            self.seadb_api.insert_rows(self.base_id, table_name, [row])
        return True

    def get_message_by_ts(self, connection_id, message_ts):
        table_name = SchemaTables.SLACK_MESSAGES.table_name(connection_id=connection_id)
        sql = f"""SELECT `_pk`, `message_id`, `channel_id`, `title`, `author`, `content`,
                `reply_count`, `latest_reply`, `created_time`, `modified_time`, `deleted`, `outdated`,
                `record_modified_time`, `sync_time`, `ai_processed_time`
                FROM `{table_name}` WHERE `message_id` = '{message_ts}'"""
        response = self.seadb_api.query_rows(self.base_id, sql)
        if response and 'results' in response and response['results']:
            return response['results'][0]
        return {}

    def mark_message_deleted(self, connection_id, message_ts):
        """Soft-delete a message by message_id without needing _pk lookup."""
        table_name = SchemaTables.SLACK_MESSAGES.table_name(connection_id=connection_id)
        now = datetime.datetime.now(datetime.UTC).isoformat()
        sql = f"""UPDATE `{table_name}` SET deleted = true,
                sync_time = '{now}',
                record_modified_time = '{now}'
                WHERE message_id = '{message_ts}' AND (deleted = false OR deleted IS NULL)"""
        return self.seadb_api.query_rows(self.base_id, sql)

    def get_all_message_ids(self, connection_id, page_size=1000):
        """Get all non-deleted message_ids (used for deletion detection)."""
        table_name = SchemaTables.SLACK_MESSAGES.table_name(connection_id=connection_id)
        message_ids = set()
        offset = 0
        while True:
            sql = f"""SELECT message_id FROM `{table_name}`
                    WHERE (deleted IS NULL OR deleted = false)
                    LIMIT {page_size} OFFSET {offset}"""
            results = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
            if not results:
                break
            for row in results:
                mid = row.get('message_id')
                if mid:
                    message_ids.add(mid)
            if len(results) < page_size:
                break
            offset += page_size
        return message_ids

    def get_message_count(self, connection_id):
        table_name = SchemaTables.SLACK_MESSAGES.table_name(connection_id=connection_id)
        sql = f"SELECT COUNT(*) as count FROM `{table_name}`"
        result = self.seadb_api.query_rows(self.base_id, sql)
        count = 0
        if result and 'results' in result and result['results']:
            count = result['results'][0].get('count', 0)
        return count

    # ---- Replies (sub-table) ----

    @classmethod
    def _reply_has_changed(cls, existing_reply, reply):
        new_content = cls.make_content(reply.get('text', ''))
        new_author = reply.get('author', '')
        edited_ts = (reply.get('edited') or {}).get('ts')
        new_modified = cls.ts_to_datetime(edited_ts) or cls.ts_to_datetime(reply.get('ts'))
        existing_modified = existing_reply.get('modified_time')
        if existing_modified:
            existing_modified = time_to_utc_time(str(existing_modified))
        return (
            (existing_reply.get('author') or '') != new_author
            or (existing_reply.get('content') or '') != new_content
            or (existing_modified is None)
            or (new_modified is not None and new_modified > existing_modified)
        )

    def add_or_update_replies(self, connection_id, message_ts, replies):
        """Insert or update thread replies for a parent message.

        ``replies`` are Slack message dicts already enriched with a resolved
        ``author`` key (the parent message itself is skipped by the caller).
        """
        table_name = SchemaTables.SLACK_MESSAGE_REPLIES.table_name(connection_id=connection_id)
        offset = 0
        page_size = 1000
        reply_id_to_pk = {}
        added_replies = []

        # Collect existing reply PKs for change detection
        while True:
            existing = self.get_replies_by_page(connection_id, message_ts, page_size, offset)
            for reply in existing:
                reply_id_to_pk[reply.get('reply_id')] = reply
            if not existing or len(existing) < page_size:
                break
            offset += page_size

        for reply in replies:
            reply_id = reply.get('ts')
            if not reply_id or reply_id == message_ts:
                continue

            text = reply.get('text', '') or ''
            author = reply.get('author', '')
            created_time = self.ts_to_datetime(reply_id)
            edited_ts = (reply.get('edited') or {}).get('ts')
            modified_time = self.ts_to_datetime(edited_ts) or created_time

            row = {
                SchemaTables.SLACK_MESSAGE_REPLIES.column.message_id.name: str(message_ts),
                SchemaTables.SLACK_MESSAGE_REPLIES.column.reply_id.name: str(reply_id),
                SchemaTables.SLACK_MESSAGE_REPLIES.column.author.name: author,
                SchemaTables.SLACK_MESSAGE_REPLIES.column.content.name: self.make_content(text),
                SchemaTables.SLACK_MESSAGE_REPLIES.column.modified_time.name: modified_time.isoformat(),
            }

            existing_reply = reply_id_to_pk.get(reply_id)
            if existing_reply:
                if self._reply_has_changed(existing_reply, reply):
                    self.seadb_api.update_rows(self.base_id, table_name, [{
                        "pk": existing_reply.get('_pk'),
                        "row": row,
                    }])
            else:
                row[SchemaTables.SLACK_MESSAGE_REPLIES.column.created_time.name] = created_time.isoformat()
                self.seadb_api.insert_rows(self.base_id, table_name, [row])
                added_replies.append({
                    'reply_id': reply_id,
                    'author': author,
                    'content': self.make_content(text),
                    'modified_time': modified_time.isoformat(),
                })

        return {
            'added': len(added_replies),
            'added_replies': added_replies,
        }

    def get_replies_by_page(self, connection_id, message_ts, page_size, offset):
        table_name = SchemaTables.SLACK_MESSAGE_REPLIES.table_name(connection_id=connection_id)
        sql = f"""SELECT `_pk`, `reply_id`, `author`, `content`, `modified_time` FROM `{table_name}`
                WHERE message_id = '{message_ts}' LIMIT {page_size} OFFSET {offset}"""
        return self.seadb_api.query_rows(self.base_id, sql).get('results', [])

    # ---- Index / AI helpers ----

    def get_messages_by_page(self, connection_id, since, page_size, offset):
        table_name = SchemaTables.SLACK_MESSAGES.table_name(connection_id=connection_id)
        sql = f"""SELECT `_pk`, `message_id`, `deleted`, `outdated`, `title`, `content`, `modified_time`
                FROM `{table_name}`
                WHERE record_modified_time >= '{since}'
                LIMIT {page_size} OFFSET {offset}"""
        response = self.seadb_api.query_rows(self.base_id, sql)
        return response.get('results', [])

    def get_replies_for_index(self, connection_id, message_ts):
        """Get all reply content for a parent message (used during search indexing)."""
        table_name = SchemaTables.SLACK_MESSAGE_REPLIES.table_name(connection_id=connection_id)
        offset = 0
        page_size = 1000
        all_content = ''
        while True:
            sql = f"""SELECT `content` FROM `{table_name}`
                    WHERE message_id = '{message_ts}' LIMIT {page_size} OFFSET {offset}"""
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
        table_name = SchemaTables.SLACK_MESSAGES.table_name(connection_id=connection_id)
        sql = f"""SELECT _pk, message_id, title, content FROM `{table_name}`
        WHERE (ai_processed_time IS NULL OR ai_processed_time < record_modified_time) AND (deleted IS NULL OR deleted = false)
        LIMIT {page_size}"""
        result = self.seadb_api.query_rows(self.base_id, sql)
        return result.get('results', [])
