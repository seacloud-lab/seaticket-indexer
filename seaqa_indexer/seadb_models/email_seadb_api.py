import datetime
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.utils.constants import SEADB_CONTENT_SIZE_LIMIT
from seaqa_indexer.utils import time_to_utc_time
import html2text
from seaqa_indexer.index.utils import EmailMessage
from seaqa_indexer.seadb_models.models import SchemaTables


class EmailSeaDBAPI:
    def __init__(self, base_id):
        self.base_id = base_id
        self.seadb_api = SeaDBAPI()

    @staticmethod
    def _normalize_subject(subject):
        if subject is None:
            return "(No Subject)"
        normalized_subject = subject.strip() if isinstance(subject, str) else str(subject).strip()
        return normalized_subject or "(No Subject)"

    def save_or_update_email_to_db(self, connection_id, email, existed_email, is_first_sync):
        if not email['content']:
            h = html2text.HTML2Text()
            h.ignore_links = False
            h.ignore_images = False
            h.body_width = 0
            markdown_text = h.handle(email['html_content']).strip().strip('```')
        else:
            markdown_text = email['content']
        
        cleaned_content = EmailMessage().parse_reply(markdown_text) if markdown_text else ''

        insert_row = {
            'email_from': email['email_from'],
            'email_to': email['email_to'],
            'message_id': email['message_id'],
            'title': email['subject'],
            'cc': email['cc'],
            'content': cleaned_content[:SEADB_CONTENT_SIZE_LIMIT],
            'reply_to_message_id': email['reply_to_message_id'],
            'is_sender': email['is_sender'],
            'email_id': email.get('email_id'),
            'origin_thread_id': email.get('origin_thread_id'),
            'deleted': False,
            'text_content': email['content'][:SEADB_CONTENT_SIZE_LIMIT],
        }

        table_name = SchemaTables.EMAIL.table_name(connection_id=connection_id)

        if existed_email:
            _pk = existed_email.get('_pk')
            insert_row['unread'] = not is_first_sync
            update_row = [
                {
                    "pk": _pk,
                    "row": insert_row,
                }
            ]
            self.seadb_api.update_rows(self.base_id, table_name, update_row)
            return _pk
        else:
            insert_row['unread'] = not is_first_sync
            resp = self.seadb_api.insert_rows(self.base_id, table_name, [insert_row])
            return resp.get('pks')[0]

    def update_html_content_and_attachments(self, connection_id, pk, email):
        table_name = SchemaTables.EMAIL.table_name(connection_id=connection_id)
        update_rows = [
            {
                "pk": pk,
                "row": {
                    'html_content': email['html_content'][:SEADB_CONTENT_SIZE_LIMIT],
                    'attachments': email['attachments'],
                    'sync_time': datetime.datetime.now(datetime.UTC).isoformat(),
                    'record_modified_time': datetime.datetime.now(datetime.UTC).isoformat(),
                    'modified_time': time_to_utc_time(email['email_date']).isoformat(),
                }
            }
        ]
        self.seadb_api.update_rows(self.base_id, table_name, update_rows)

    def update_email_thread_id(self, connection_id, message_id, thread_id):
        table_name = SchemaTables.EMAIL.table_name(connection_id=connection_id)

        sql = f"SELECT _pk FROM `{table_name}` WHERE `message_id`='{message_id}'"
        results = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
        if results:
            _pk = results[0].get('_pk')
            update_row = [
                {
                "pk":_pk,
                "row": {'thread_id': thread_id},
                }
            ]
            self.seadb_api.update_rows(self.base_id, table_name, update_row)

    def save_thread_to_db(self, connection_id, thread):
        insert_row = {
            SchemaTables.THREAD.column.title.name: self._normalize_subject(thread.get('subject')),
            SchemaTables.THREAD.column.modified_time.name: time_to_utc_time(thread['last_updated_at']).isoformat(),
            SchemaTables.THREAD.column.unread.name: thread.get('unread'),
            SchemaTables.THREAD.column.is_replied.name: False,
            SchemaTables.THREAD.column.sync_time.name: datetime.datetime.now(datetime.UTC).isoformat(),
            SchemaTables.THREAD.column.record_modified_time.name: datetime.datetime.now(datetime.UTC).isoformat(),
            SchemaTables.THREAD.column.deleted.name: False
        }

        table_name = SchemaTables.THREAD.table_name(connection_id=connection_id)
        return self.seadb_api.insert_rows(self.base_id, table_name, [insert_row]).get('pks')[0]

    def update_thread_to_db(self, connection_id, thread):
        update_row = {}

        if thread.get('last_updated_at'):
            update_row['modified_time'] = time_to_utc_time(thread['last_updated_at']).isoformat()
        if thread.get('unread') is not None:
            update_row['unread'] = thread.get('unread')
        update_row['sync_time'] = datetime.datetime.now(datetime.UTC).isoformat()

        table_name = SchemaTables.THREAD.table_name(connection_id=connection_id)
        _pk = thread['thread_id']
        update_row = [
            {
            "pk":_pk,
            "row":update_row,
            }
        ]
        self.seadb_api.update_rows(self.base_id, table_name, update_row)

    def get_email_total_count(self, connection_id):
        table_name = SchemaTables.EMAIL.table_name(connection_id=connection_id)
        sql = f"SELECT COUNT(*) as count FROM `{table_name}`"
        result = self.seadb_api.query_rows(self.base_id, sql)
        return result['results'][0].get('count', 0)

    def get_threads_by_page(self, connection_id, since, page_size, offset):
        table_name = SchemaTables.THREAD.table_name(connection_id=connection_id)
        sql = f"""SELECT `_pk`, `deleted`, `outdated`, `title`, `modified_time` FROM `{table_name}` 
                WHERE record_modified_time >= '{since}' 
                LIMIT {page_size} OFFSET {offset}"""
        res = self.seadb_api.query_rows(self.base_id, sql)
        results = res.get('results', [])
        return results

    def get_threaded_emails_by_page(self, connection_id, since, page_size, offset):
        table_name = SchemaTables.EMAIL.table_name(connection_id=connection_id)
        sql = f"SELECT message_id, thread_id FROM `{table_name}` WHERE sync_time >= '{since}' and thread_id is not null LIMIT {page_size} OFFSET {offset}"
        res = self.seadb_api.query_rows(self.base_id, sql)
        results = res.get('results', [])
        return results

    def get_thread_emails_by_message_ids(self, connection_id, message_ids):
        message_ids_str = ', '.join([f"'{message_id}'" for message_id in message_ids])
        table_name = SchemaTables.EMAIL.table_name(connection_id=connection_id)
        sql = f"SELECT message_id, thread_id FROM `{table_name}` WHERE `message_id`in ({message_ids_str}) and thread_id is not null"
        res = self.seadb_api.query_rows(self.base_id, sql)
        results = res.get('results', [])
        return results

    def delete_threads_from_db(self, connection_id, deleted_seadb_pk_list):
        table_name = SchemaTables.THREAD.table_name(connection_id=connection_id)
        self.seadb_api.delete_rows(self.base_id, table_name, deleted_seadb_pk_list)

        email_table_name = SchemaTables.EMAIL.table_name(connection_id=connection_id)
        for thread_id in deleted_seadb_pk_list:
            delete_sql = f"DELETE FROM `{email_table_name}` WHERE thread_id = {thread_id}"
            self.seadb_api.query_rows(self.base_id, delete_sql)

    def get_email_by_message_id(self, connection_id, message_id):
        table_name = SchemaTables.EMAIL.table_name(connection_id=connection_id)
        sql = f"SELECT _pk, modified_time, thread_id FROM `{table_name}` WHERE message_id='{message_id}'"
        res = self.seadb_api.query_rows(self.base_id, sql)
        result = res.get('results', [])
        return result[0] if result else {}

    def get_thread_by_pk(self, connection_id, thread_id):
        table_name = SchemaTables.THREAD.table_name(connection_id=connection_id)
        sql = f"SELECT _pk, title, modified_time, unread, linked_ticket, deleted, tags FROM `{table_name}` WHERE `_pk` = {thread_id} LIMIT 1"
        res = self.seadb_api.query_rows(self.base_id, sql)
        result = res.get('results', [])
        return result[0] if result else {}

    def get_thread_emails_by_page(self, connection_id, thread_id, page_size, offset):
        email_table_name = SchemaTables.EMAIL.table_name(connection_id=connection_id)
        sql_comments = f"SELECT `content` FROM `{email_table_name}` WHERE thread_id = {thread_id} LIMIT {page_size} OFFSET {offset}"
        comments = self.seadb_api.query_rows(self.base_id, sql_comments).get('results', [])
        return comments

    def get_need_ai_emails_by_page(self, connection_id, page_size):
        thread_table_name = SchemaTables.THREAD.table_name(connection_id=connection_id)
        sql = f"""
            SELECT `_pk`, `title` FROM `{thread_table_name}`
            WHERE (ai_processed_time IS NULL OR ai_processed_time < record_modified_time) 
            AND (deleted IS NULL OR deleted = false)
            LIMIT {page_size}
        """
        res = self.seadb_api.query_rows(self.base_id, sql)
        emails = res.get('results', [])
        return emails

    def get_first_email(self, connection_id, thread_id):
        table_name = SchemaTables.EMAIL.table_name(connection_id=connection_id)
        if not thread_id:
            return {}

        sql = f"SELECT content FROM `{table_name}` WHERE thread_id = {thread_id} ORDER BY modified_time ASC LIMIT 1"
        result = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
        if result:
            return result[0]
        return {}

    def update_thread_is_replied(self, connection_id, thread_id, is_replied):
        thread_table = SchemaTables.THREAD.table_name(connection_id=connection_id)
        self.seadb_api.update_rows(self.base_id, thread_table, [
            {"pk": thread_id, "row": {"is_replied": is_replied}}
        ])

    def recompute_thread_is_replied(self, connection_id, thread_id):
        """Recompute is_replied from the last non-deleted email in a thread.

        A thread is replied only when its last email was sent by the connected
        mailbox and references another message as a reply.
        """
        email_table = SchemaTables.EMAIL.table_name(connection_id=connection_id)
        sql = f"""
            SELECT is_sender, reply_to_message_id FROM `{email_table}`
            WHERE thread_id = {thread_id}
            AND deleted = false
            ORDER BY modified_time DESC, _pk DESC LIMIT 1
        """
        result = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
        last_email = result[0] if result else {}
        is_replied = bool(last_email.get('is_sender') and last_email.get('reply_to_message_id'))
        self.update_thread_is_replied(connection_id, thread_id, is_replied)
