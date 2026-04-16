import json
import logging
import datetime

from dateutil.relativedelta import relativedelta

from seaqa_indexer.handlers.base_handler import ConnectionHandler
from seaqa_indexer.seadb_models.models import EmailTable, ThreadTable
from seaqa_indexer.utils.constants import ConnectionType
from seaqa_indexer.syncer.utils import upload_email_attachment, insert_and_update_thread_rows, EMAIL_SYNC_INTERVAL_DAYS
from seaqa_indexer.utils.imap_mail import ImapMail
from seaqa_indexer.seadb_models.email_seadb_api import EmailSeaDBAPI
from seaqa_indexer.crawler.storage import get_storage
from seaqa_indexer.index.utils import gen_index_name_by_type
from seaqa_indexer.utils.constants import IndexStatus
from seaqa_indexer.utils import need_update_connection_last_sync_count, time_to_utc_time
from seaqa_indexer.index.index_store.email_index import EmailIndex
from seaqa_indexer.ai_processor.utils import generate_summary

logger = logging.getLogger(__name__)


class EmailHandler(ConnectionHandler):
    """Handler for EMAIL connection type."""

    connection_type = ConnectionType.EMAIL.value
    BATCH_SIZE = 50

    def gen_table_name(self, connection_id) -> str:
        return ThreadTable.gen_table_name(connection_id)

    def gen_email_table_name(self, connection_id) -> str:
        return EmailTable.gen_table_name(connection_id)

    def get_search_param(self, seasearch_api, index_name, keyword, start, size, time_from, time_to):
        email_index = EmailIndex(seasearch_api)
        return email_index.get_search_param(index_name, keyword, start, size, time_from, time_to)

    def delete_index_by_connection_id(self, seasearch_api, connection_id):
        index_name = gen_index_name_by_type(connection_id, self.connection_type)
        email_index = EmailIndex(seasearch_api)
        email_index.delete_index_by_index_name(index_name)

        # summary vector index
        self.delete_summary_vector_index_by_connection_id(seasearch_api, connection_id)

    def format_vector_search_result(self, record, connection, result):
        return {
            'title': record.get('title', ''),
        }

    def process_ai(self, seadb_api, storage, connection_id, project_uuid):
        page_size = 100
        updates = []
        table_name = self.gen_table_name(connection_id)
        email_seadb_api = EmailSeaDBAPI(project_uuid)

        ai_processing_count = 0
        while True:
            rows = email_seadb_api.get_need_ai_emails_by_page(connection_id, page_size)
            if not rows:
                break
            for row in rows:
                pk = row.get('_pk')
                title = row.get('title', '')
                result = email_seadb_api.get_first_email(connection_id, pk)
                content = result.get('content') or ''

                full_content = f"Title: {title}\n\nBody: {content}\n"
                summary, vector = generate_summary(full_content, ConnectionType.EMAIL.value, project_uuid)
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

        email_seadb_api = EmailSeaDBAPI(project_uuid)
        email_index = EmailIndex(seasearch_api)

        connection_status['last_index_status'] = IndexStatus.INDEXING
        seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
        index_name = gen_index_name_by_type(connection_id, connection_type)
        email_index.create_index_if_missing(index_name)

        index_count = email_index.update(index_name, connection_id, connection_status, seaqa_db, email_seadb_api, since)
        indexed_at = datetime.datetime.now(datetime.UTC)
        connection_status['last_indexed_count'] = index_count
        connection_status['last_index_status'] = IndexStatus.COMPLETED
        seaqa_db.update_connection(connection_id, indexed_at, json.dumps(connection_status))

    def sync(self, connection_id, project_uuid, config, old_last_sync_time, connection_status, seaqa_db, sync_logger):
        imap_host = config.get('imap_host')
        imap_port = config.get('imap_port')
        email_user = config.get('username')
        email_password = config.get('password')
        sync_years = config.get('sync_years', 5)
        since = old_last_sync_time
        is_first_update = False

        if not since:
            since = datetime.datetime.now(datetime.UTC) - relativedelta(years=sync_years)
            is_first_update = True
        since = time_to_utc_time(str(since)).isoformat()

        email_seadb_api = EmailSeaDBAPI(project_uuid)
        storage = get_storage()

        try:
            imap = ImapMail(imap_host, email_user, email_password, port=imap_port)
        except Exception as e:
            logger.error("connection_id %s imap_host: %s, email_user: %s, login error: %s",
                        connection_id, imap_host, email_user, e)
            sync_logger.log(f"imap account is invalid")
            return None, None, None, None

        sync_count, total_count = self._sync_email(connection_id, project_uuid, connection_status, since,
                         imap, seaqa_db, email_seadb_api, storage, is_first_update)
        return sync_count, total_count, False, config

    def clean_deleted_records(self, seadb_api, project_uuid, connection_id, cutoff_time):
        """Clean deleted email thread records and their emails."""
        page_size = 1000
        table_name = self.gen_table_name(connection_id)
        email_table_name = self.gen_email_table_name(connection_id)
        total_deleted = 0

        while True:
            sql = f"""
                SELECT _pk FROM `{table_name}` 
                WHERE deleted = true AND sync_time < '{cutoff_time}'
                LIMIT {page_size}
            """
            result = seadb_api.query_rows(project_uuid, sql)
            rows = result.get('results', [])
            if not rows:
                break

            deleted_pks = [row.get('_pk') for row in rows]

            # Delete from thread table
            seadb_api.delete_rows(project_uuid, table_name, deleted_pks)

            # Delete related emails
            for thread_id in deleted_pks:
                delete_sql = f"DELETE FROM `{email_table_name}` WHERE thread_id = {thread_id}"
                seadb_api.query_rows(project_uuid, delete_sql)

            total_deleted += len(deleted_pks)

            if len(rows) < page_size:
                break

        return total_deleted

    def clean_connection_data(self, storage, seadb_api, project_uuid,
                              connection_id, tables):
        """Clean email connection data including attachment storage and tables."""
        # Delete attachment storage (uses same method as site)
        storage.delete_site(connection_id, project_uuid)
        # Delete seadb tables
        if tables:
            email_table_name = EmailTable.gen_table_name(connection_id)
            thread_table_name = ThreadTable.gen_table_name(connection_id)
            for table in tables:
                if table['name'] in (email_table_name, thread_table_name):
                    seadb_api.delete_table(project_uuid, table['id'])

    def _sync_email(self, connection_id, project_uuid, connection_status, since, imap, seaqa_db, email_seadb_api, storage, is_first_sync):
        sync_count = 0
        added_count = 0
        updated_count = 0
        _status_last_update_time = datetime.datetime.now()
        _status_last_update_count = 0

        since = time_to_utc_time(str(since))
        today = datetime.datetime.now(datetime.timezone.utc).date()

        # Due to time zone differences, emails sent on the same day may not be available for retrieval within that time frame.
        # Therefore, the search scope has been expanded to include a full day.
        before_date = since.date() - datetime.timedelta(days=1)

        while before_date <= today:
            since_date = before_date
            before_date += datetime.timedelta(days=EMAIL_SYNC_INTERVAL_DAYS)

            email_list = imap.search_emails(since_date, before_date)
            new_email_list = []
            for email in email_list:
                if not email:
                    continue
                existed_email = {}
                if not is_first_sync:
                    message_id = email.get('message_id')
                    modified_time = email.get('email_date')
                    existed_email = email_seadb_api.get_email_by_message_id(connection_id, message_id)
                    if existed_email:
                        existed_date = time_to_utc_time(str(existed_email.get('modified_time')))
                        email_thread_id = existed_email.get('thread_id')
                        if time_to_utc_time(modified_time) <= existed_date and email_thread_id:
                            logger.debug(f'connection_id %s message_id %s is up to date, skipping sync.', connection_id, message_id)
                            continue
                email_info = {
                    'message_id': email.get('message_id'),
                    'reply_to_message_id': email.get('reply_to_message_id'),
                    'email_date': email.get('email_date'),
                    'subject': email.get('subject'),
                    'is_sender': email.get('is_sender'),
                }
                new_email_list.append(email_info)

                pk = email_seadb_api.save_or_update_email_to_db(connection_id, email, existed_email)
                upload_email_attachment(connection_id, project_uuid, storage, email, pk)
                email_seadb_api.update_html_content_and_attachments(connection_id, pk, email)
                if existed_email:
                    updated_count += 1
                else:
                    added_count += 1

                should, new_time, new_count = need_update_connection_last_sync_count(
                    sync_count, _status_last_update_time, _status_last_update_count
                )

                if should:
                    connection_status['last_sync_count'] = sync_count
                    seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
                    _status_last_update_time = new_time
                    _status_last_update_count = new_count
                sync_count += 1
            if new_email_list:
                insert_and_update_thread_rows(email_seadb_api, connection_id, since, new_email_list, is_first_sync)

        connection_status['last_sync_changes'] = {
            'added': added_count,
            'deleted': 0,
            'updated': updated_count,
        }
        total_count = email_seadb_api.get_email_total_count(connection_id)
        return sync_count, total_count
