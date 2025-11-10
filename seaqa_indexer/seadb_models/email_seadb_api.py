import datetime
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.seadb_models.models import EmailTable
from seaqa_indexer.utils.constants import SEADB_CONTENT_SIZE_LIMIT
from seaqa_indexer.utils import time_to_utc_time


class EmailSeaDBAPI:
    def __init__(self, base_id):
        self.base_id = base_id
        self.seadb_api = SeaDBAPI()

    def save_or_update_email_to_db(self, connection_id, email):
        insert_row = {
            'email_from': email['email_from'],
            'email_to': email['email_to'],
            'message_id': email['message_id'],
            'title': email['title'],
            'cc': email['cc'],
            'content': email['content'][:SEADB_CONTENT_SIZE_LIMIT],
            'html_content': email['html_content'][:SEADB_CONTENT_SIZE_LIMIT],
            'attachment': email['attachment'],
            'modified_time': time_to_utc_time(email['modified_time']).isoformat(),
            'reply_to_message_id': email['reply_to_message_id'],
            'is_sender': email['is_sender'],
            'sync_time': datetime.datetime.now(datetime.UTC).isoformat(),
            'deleted': False,
        }

        table_name = EmailTable.gen_table_name(connection_id)
        message_id = email['message_id']

        sql = f"SELECT * FROM `{table_name}` WHERE `message_id`='{message_id}'"
        result = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
        if result:
            _pk = result[0].get('_pk')
            update_row = [
                {
                "pk":_pk,
                "row":insert_row,
                }
            ]
            self.seadb_api.update_rows(self.base_id, table_name, update_row)
        else:
            self.seadb_api.insert_rows(self.base_id, table_name, [insert_row])

    def get_email_total_count(self, connection_id):
        table_name = EmailTable.gen_table_name(connection_id)
        sql = f"SELECT COUNT(*) as count FROM `{table_name}`"
        result = self.seadb_api.query_rows(self.base_id, sql)
        return result['results'][0].get('count', 0)

    def get_emails_by_page(self, connection_id, since, page_size, offset):
        table_name = EmailTable.gen_table_name(connection_id)
        sql = f"SELECT * FROM `{table_name}` WHERE sync_time >= '{since}' LIMIT {page_size} OFFSET {offset}"
        res = self.seadb_api.query_rows(self.base_id, sql)
        results = res.get('results', [])
        return results

    def delete_emails_from_db(self, connection_id, deleted_seadb_pk_list):
        table_name = EmailTable.gen_table_name(connection_id)
        self.seadb_api.delete_rows(self.base_id, table_name, deleted_seadb_pk_list)

    def get_email_by_message_id(self, connection_id, message_id):
        table_name = EmailTable.gen_table_name(connection_id)
        sql = f"SELECT email_date FROM `{table_name}` WHERE message_id='{message_id}'"
        res = self.seadb_api.query_rows(self.base_id, sql)
        result = res.get('results', [])
        return result[0] if result else None

