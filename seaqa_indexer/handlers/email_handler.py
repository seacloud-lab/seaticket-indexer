import json
import logging
import datetime

from dateutil.relativedelta import relativedelta

from seaqa_indexer.handlers.base_handler import ConnectionHandler
from seaqa_indexer.seadb_models.models import EmailTable, ThreadTable
from seaqa_indexer.utils.constants import ConnectionType

logger = logging.getLogger(__name__)


class EmailHandler(ConnectionHandler):
    """Handler for EMAIL connection type."""
    
    connection_type = ConnectionType.EMAIL.value
    table_class = ThreadTable
    
    def get_index_instance(self, seasearch_api):
        from seaqa_indexer.index.index_store.email_index import EmailIndex
        return EmailIndex(seasearch_api)
    
    def get_search_param(self, index_instance, index_name, keyword, start, size, time_from, time_to):
        return index_instance.get_search_param(index_name, keyword, start, size, time_from, time_to)
    
    def delete_index(self, index_manager, connection_id):
        from seaqa_indexer.index.utils import gen_index_name_by_type
        index_name = gen_index_name_by_type(connection_id, self.connection_type)
        index_manager.email_index.delete_index_by_index_name(index_name)
    
    def format_vector_search_result(self, record, connection, result):
        return {
            'title': record.get('title', ''),
            'modified_time': record.get('modified_time', ''),
        }
    
    def process_ai(self, seadb_api, storage, connection_id, project_uuid):
        from seaqa_indexer.ai_processor.ai_processor import process_email_ai
        return process_email_ai(seadb_api, connection_id, project_uuid)
    
    def update_index(self, connection, seasearch_api, seaqa_db):
        from seaqa_indexer.index.index_utils import update_email_index
        update_email_index(connection, seasearch_api, seaqa_db)
    
    def sync(self, connection_id, project_uuid, config, old_last_sync_time, 
             connection_status, seaqa_db, sync_logger):
        from seaqa_indexer.utils import time_to_utc_time
        from seaqa_indexer.utils.imap_mail import ImapMail
        from seaqa_indexer.seadb_models.email_seadb_api import EmailSeaDBAPI
        from seaqa_indexer.syncer.sync_connection import sync_email
        from seaqa_indexer.crawler.storage import get_storage
        
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
            return None, None
        
        sync_count, total_count = sync_email(connection_id, project_uuid, connection_status, since, 
                         imap, seaqa_db, email_seadb_api, storage, is_first_update)
        return sync_count, total_count, False, config
    
    def clean_deleted_records(self, seadb_api, project_uuid, connection_id, 
                              table_name, cutoff_time):
        """Clean deleted email thread records and their emails."""
        page_size = 1000
        email_table_name = EmailTable.gen_table_name(connection_id)
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
    
    def add_sync_task(self, app, connection_id):
        app.sync_manager.add_sync_task(connection_id)
    
    def add_recovering_sync_task(self, mq, connection_id):
        msg_content = {
            'connection_id': connection_id,
        }
        mq.lpush('sync_task', json.dumps(msg_content))
