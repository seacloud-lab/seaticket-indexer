import os
import time
import logging
import threading
import signal
import datetime

from seaqa_indexer import config
from seaqa_indexer.app.log import LogConfigurator
from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.mq import get_mq
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.utils.seasearch_api import SeaSearchAPI
from seaqa_indexer.utils.constants import ConnectionType
from seaqa_indexer.seadb_models.models import (
    WebCrawlTable, GithubIssuesTable, GithubIssueCommentsTable,
    DiscourseTopicsTable, DiscourseRepliesTable, SeafileTable, EmailTable, ThreadTable
)


logger = logging.getLogger(__name__)


CONNECTION_TYPE_TO_TABLE = {
    ConnectionType.SITE.value: WebCrawlTable,
    ConnectionType.GITHUB_ISSUE.value: GithubIssuesTable,
    ConnectionType.DISCOURSE_FORUM.value: DiscourseTopicsTable,
    ConnectionType.SEAFILE.value: SeafileTable,
    ConnectionType.EMAIL.value: EmailTable,
}


class DeletedRecordsCleaner(object):
    """
    Clean deleted records for recently active connections.
    """
    
    def __init__(self):
        self.db_session_class = init_db_session_class()
        self.seaqa_db = SeaqaDB(self.db_session_class)
        self.seadb_api = SeaDBAPI()
        self.seasearch_api = SeaSearchAPI(config.SEASEARCH_URL, config.SEASEARCH_TOKEN)
        
        self.should_stop = threading.Event()
        self.clear_interval = 24 * 60 * 60
        self.recent_days = 14  # Only check connections updated in the last 14 days
        self.delete_after_days = 14  # Only delete records that have been deleted for more than 14 days
        
        self.mq = get_mq(config.REDIS_HOST, config.REDIS_PORT, config.REDIS_PASSWORD)
        
        self.set_signal()

    @property
    def tname(self):
        return threading.current_thread().name

    def start(self):
        if not self.mq:
            logger.warning('MQ not available, cannot start deleted records cleaner')
            return
        threading.Thread(
            target=self.cron_clean_deleted_records, 
            name='cron_clean_deleted_records_thread', 
            daemon=True
        ).start()

    def cron_clean_deleted_records(self):
        logger.info('Start deleted records cleaner timer')
        while not self.should_stop.is_set():
            self.should_stop.wait(self.clear_interval)

            if not self.should_stop.is_set():
                logger.info('Start to clean deleted records')
                try:
                    self.clean_deleted_records()
                except Exception as e:
                    logger.exception('Cron clean deleted records error: %s', e)

    def clean_deleted_records(self):
        connections = self.seaqa_db.get_recently_updated_connections(days=self.recent_days)
        logger.info('Found %d recently updated connections to check for deleted records', len(connections))
        
        for connection in connections:
            connection_id = connection.id
            connection_type = connection.type
            project_uuid = connection.project_uuid
            
            try:
                table_class = CONNECTION_TYPE_TO_TABLE.get(connection_type)
                if not table_class:
                    logger.debug('Unsupported connection type for cleaning: %s', connection_type)
                    continue
                
                table_name = table_class.gen_table_name(connection_id)
                
                if connection_type == ConnectionType.DISCOURSE_FORUM.value:
                    self._clean_deleted_discourse_records(project_uuid, connection_id, table_name)
                elif connection_type == ConnectionType.GITHUB_ISSUE.value:
                    self._clean_deleted_github_records(project_uuid, connection_id, table_name)
                elif connection_type == ConnectionType.EMAIL.value:
                    self._clean_deleted_email_records(project_uuid, connection_id, table_name)
                else:
                    self._clean_deleted_records(project_uuid, connection_id, table_name)
            except Exception as e:
                logger.exception(
                    'Failed to clean deleted records for connection %s: %s', 
                    connection_id, e
                )

    def _clean_deleted_records(self, project_uuid, connection_id, table_name):
        """Query and delete records in batches."""
        page_size = 1000
        cutoff_time = (datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=self.delete_after_days)).strftime('%Y-%m-%d %H:%M:%S')
        total_deleted = 0
        
        while True:
            sql = f"""
                SELECT _pk FROM `{table_name}` 
                WHERE deleted = true AND sync_time < '{cutoff_time}'
                LIMIT {page_size}
            """
            try:
                result = self.seadb_api.query_rows(project_uuid, sql)
                rows = result.get('results', [])
                if not rows:
                    break
                
                deleted_pks = [row.get('_pk') for row in rows]
                self.seadb_api.delete_rows(project_uuid, table_name, deleted_pks)
                total_deleted += len(deleted_pks)
                
                if len(rows) < page_size:
                    break
            except Exception as e:
                logger.warning('Failed to clean deleted records from %s: %s', table_name, e)
                break
        
        if total_deleted > 0:
            logger.info('Deleted %d records from connection %s table %s', total_deleted, connection_id, table_name)

    def _clean_deleted_discourse_records(self, project_uuid, connection_id, table_name):
        """Query and delete discourse topic records and their replies in batches."""
        page_size = 1000
        cutoff_time = (datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=self.delete_after_days)).strftime('%Y-%m-%d %H:%M:%S')
        replies_table_name = DiscourseRepliesTable.gen_table_name(connection_id)
        total_deleted = 0
        
        while True:
            sql = f"""
                SELECT _pk, topic_id FROM `{table_name}` 
                WHERE deleted = true AND sync_time < '{cutoff_time}'
                LIMIT {page_size}
            """
            try:
                result = self.seadb_api.query_rows(project_uuid, sql)
                rows = result.get('results', [])
                if not rows:
                    break
                
                deleted_pks = [row.get('_pk') for row in rows]
                deleted_topic_ids = [row.get('topic_id') for row in rows]
                
                self.seadb_api.delete_rows(project_uuid, table_name, deleted_pks)
                for topic_id in deleted_topic_ids:
                    delete_sql = f"DELETE FROM `{replies_table_name}` WHERE topic_id = {topic_id}"
                    self.seadb_api.query_rows(project_uuid, delete_sql)
                
                total_deleted += len(deleted_pks)
                
                if len(rows) < page_size:
                    break
            except Exception as e:
                logger.warning('Failed to clean deleted discourse records from %s: %s', table_name, e)
                break
        
        if total_deleted > 0:
            logger.info('Deleted %d discourse records from connection %s', total_deleted, connection_id)

    def _clean_deleted_github_records(self, project_uuid, connection_id, table_name):
        """Query and delete github issue records and their comments in batches."""
        page_size = 1000
        cutoff_time = (datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=self.delete_after_days)).strftime('%Y-%m-%d %H:%M:%S')
        comments_table_name = GithubIssueCommentsTable.gen_table_name(connection_id)
        total_deleted = 0
        
        while True:
            sql = f"""
                SELECT _pk, issue_id FROM `{table_name}` 
                WHERE deleted = true AND sync_time < '{cutoff_time}'
                LIMIT {page_size}
            """
            try:
                result = self.seadb_api.query_rows(project_uuid, sql)
                rows = result.get('results', [])
                if not rows:
                    break
                
                deleted_pks = [row.get('_pk') for row in rows]
                deleted_issue_ids = [row.get('issue_id') for row in rows]
                
                self.seadb_api.delete_rows(project_uuid, table_name, deleted_pks)
                for issue_id in deleted_issue_ids:
                    delete_sql = f"DELETE FROM `{comments_table_name}` WHERE issue_id = {issue_id}"
                    self.seadb_api.query_rows(project_uuid, delete_sql)
                
                total_deleted += len(deleted_pks)
                
                if len(rows) < page_size:
                    break
            except Exception as e:
                logger.warning('Failed to clean deleted github records from %s: %s', table_name, e)
                break
        
        if total_deleted > 0:
            logger.info('Deleted %d github issue records from connection %s', total_deleted, connection_id)

    def _clean_deleted_email_records(self, project_uuid, connection_id, table_name):
        """Query and delete email thread records and their emails in batches."""
        page_size = 1000
        cutoff_time = (datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=self.delete_after_days)).strftime('%Y-%m-%d %H:%M:%S')
        email_table_name = EmailTable.gen_table_name(connection_id)
        total_deleted = 0
        
        while True:
            sql = f"""
                SELECT _pk FROM `{table_name}` 
                WHERE deleted = true AND sync_time < '{cutoff_time}'
                LIMIT {page_size}
            """
            try:
                result = self.seadb_api.query_rows(project_uuid, sql)
                rows = result.get('results', [])
                if not rows:
                    break
                
                deleted_pks = [row.get('_pk') for row in rows]
                
                # Delete from thread table
                thread_table_name = ThreadTable.gen_table_name(connection_id)
                self.seadb_api.delete_rows(project_uuid, thread_table_name, deleted_pks)
                
                # Delete related emails
                for thread_id in deleted_pks:
                    delete_sql = f"DELETE FROM `{email_table_name}` WHERE thread_id = {thread_id}"
                    self.seadb_api.query_rows(project_uuid, delete_sql)
                
                total_deleted += len(deleted_pks)
                
                if len(rows) < page_size:
                    break
            except Exception as e:
                logger.warning('Failed to clean deleted email records from %s: %s', table_name, e)
                break
        
        if total_deleted > 0:
            logger.info('Deleted %d email thread records from connection %s', total_deleted, connection_id)

    def clear(self):
        if not self.mq:
            return
        self.should_stop.set()
        logger.info("Exit deleted records cleaner process")
        os._exit(0)

    def signal_term_handler(self, signal, frame):
        self.clear()

    def set_signal(self):
        signal.signal(signal.SIGTERM, self.signal_term_handler)


def start():
    deleted_records_cleaner = DeletedRecordsCleaner()
    logger.info("Deleted records cleaner process initialized.")
    try:
        deleted_records_cleaner.start()
    except Exception as e:
        logger.exception(e)
        deleted_records_cleaner.clear()
        os._exit(0)

    while True:
        time.sleep(2)


def main():
    LogConfigurator(config.LOG_LEVEL, config.LOG_FILE)
    start()


if __name__ == "__main__":
    main()
