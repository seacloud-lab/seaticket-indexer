import logging
import datetime
from threading import Thread, Event

from seaqa_indexer import config
from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.utils.seadb_api import SeaDBAPI, TableNotFound
from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.utils.seasearch_api import SeaSearchAPI
from seaqa_indexer.handlers import get_handler
from seaqa_indexer.seadb_models.utils import get_table_name_from_schema

from seaqa_indexer.seadb_models.models import SchemaTableNames



logger = logging.getLogger(__name__)

class DeletedRecordsCleaner(object):
    def __init__(self):
        self._enabled = True
        self.interval = 24 * 60 * 60  # Running interval
        self.check_days = 7  # Only check connections updated in the last 14 days
        self.delete_after_days = 14  # Only delete records that have been deleted for more than 14 days

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start deleted records cleaner: it is not enabled!')
            return

        DeletedRecordsCleanerTimer(self.interval, self.check_days, self.delete_after_days).start()
        logging.info('deleted records cleaner timer is started')

    def is_enabled(self):
        return self._enabled

class DeletedRecordsCleanerTimer(Thread):
    """
    Clean deleted records for recently active connections.
    """

    def __init__(self, interval, check_days, delete_after_days):
        Thread.__init__(self)
        self.db_session_class = init_db_session_class()
        self.seaqa_db = SeaqaDB(self.db_session_class)
        self.seadb_api = SeaDBAPI()
        self.seasearch_api = SeaSearchAPI(config.SEASEARCH_URL, config.SEASEARCH_TOKEN)

        self.finished = Event()
        self.interval = interval
        self.check_days = check_days
        self.delete_after_days = delete_after_days

    
    def clean_deleted_portal_issue_records(self, seadb_api, project_uuid, cutoff_time):
        page_size = 1000
        portal_issue_table_name = get_table_name_from_schema(SchemaTableNames.PORTAL_ISSUES)
        portal_issue_comment_table_name = get_table_name_from_schema(SchemaTableNames.PORTAL_ISSUE_COMMENTS)
        total_deleted = 0

        while True:
            sql = f"""
                SELECT _pk FROM `{portal_issue_table_name}` 
                WHERE deleted = true AND modified_time < '{cutoff_time}'
                LIMIT {page_size}
            """
            try:
                result = seadb_api.query_rows(project_uuid, sql)
            except TableNotFound:
                logger.warning('table %s not found', portal_issue_table_name)
                break
            rows = result.get('results', [])
            if not rows:
                break

            deleted_pks = [row.get('_pk') for row in rows]

            seadb_api.delete_rows(project_uuid, portal_issue_table_name, deleted_pks)
            for issue_id in deleted_pks:
                delete_sql = f"DELETE FROM `{portal_issue_comment_table_name}` WHERE issue_id = {issue_id}"
                seadb_api.query_rows(project_uuid, delete_sql)

            total_deleted += len(deleted_pks)

            if len(rows) < page_size:
                break

        return total_deleted
    
    def clean_deleted_ticket_records(self, seadb_api, project_uuid, cutoff_time):
        page_size = 1000
        ticket_table_name = get_table_name_from_schema(SchemaTableNames.TICKETS)
        ticket_comment_table_name = get_table_name_from_schema(SchemaTableNames.TICKET_COMMENTS)
        total_deleted = 0

        while True:
            sql = f"""
                SELECT _pk FROM `{ticket_table_name}` 
                WHERE deleted = true AND modified_time < '{cutoff_time}'
                LIMIT {page_size}
            """
            result = seadb_api.query_rows(project_uuid, sql)
            rows = result.get('results', [])
            if not rows:
                break

            deleted_pks = [row.get('_pk') for row in rows]

            seadb_api.delete_rows(project_uuid, ticket_table_name, deleted_pks)
            for issue_id in deleted_pks:
                delete_sql = f"DELETE FROM `{ticket_comment_table_name}` WHERE ticket_id = {issue_id}"
                seadb_api.query_rows(project_uuid, delete_sql)

            total_deleted += len(deleted_pks)

            if len(rows) < page_size:
                break

        return total_deleted
    
    def clean_deleted_kb_records(self, seadb_api, project_uuid, cutoff_time):
        page_size = 1000
        kb_table_name = get_table_name_from_schema(SchemaTableNames.KNOWLEDGE_BASE)
        total_deleted = 0

        while True:
            sql = f"""
                SELECT _pk FROM `{kb_table_name}` 
                WHERE deleted = true AND modified_time < '{cutoff_time}'
                LIMIT {page_size}
            """
            result = seadb_api.query_rows(project_uuid, sql)
            rows = result.get('results', [])
            if not rows:
                break

            deleted_pks = [row.get('_pk') for row in rows]

            seadb_api.delete_rows(project_uuid, kb_table_name, deleted_pks)

            total_deleted += len(deleted_pks)

            if len(rows) < page_size:
                break

        return total_deleted

    def clean_deleted_project_records(self):
        projects = self.seaqa_db.get_recently_updated_projects(days=self.check_days)
        logger.info('Found %d recently updated projects to check for deleted records', len(projects))
        cutoff_time = (datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=self.delete_after_days)).isoformat()

        for project in projects:
            project_uuid = project.uuid

            try:
                deleted_portal_issue_count = self.clean_deleted_portal_issue_records(self.seadb_api, project_uuid, cutoff_time)
                deleted_ticket_count = self.clean_deleted_ticket_records(self.seadb_api, project_uuid, cutoff_time)
                deleted_kb_count = self.clean_deleted_kb_records(self.seadb_api, project_uuid, cutoff_time)
                
                logger.info('Deleted %d portal issue records, %d ticket records, %d konwledge_base records from project %s',  
                            deleted_portal_issue_count, deleted_ticket_count, deleted_kb_count, project_uuid)
            except Exception as e:
                logger.exception('Failed to clean deleted records for project %s: %s', project_uuid, e)

    def clean_deleted_connection_records(self):
        connections = self.seaqa_db.get_recently_updated_connections(days=self.check_days)
        logger.info('Found %d recently updated connections to check for deleted records', len(connections))
        cutoff_time = (datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=self.delete_after_days)).isoformat()

        for connection in connections:
            connection_id = connection.id
            connection_type = connection.type
            project_uuid = connection.project_uuid

            try:
                handler = get_handler(connection_type)
                if not handler:
                    logger.debug('Unsupported connection type for cleaning: %s', connection_type)
                    continue

                table_name = handler.gen_table_name(connection_id)
                total_deleted = handler.clean_deleted_records(
                    self.seadb_api, project_uuid, connection_id, cutoff_time
                )
                logger.info('Deleted %d records from connection %s table %s',  total_deleted, connection_id, table_name)
            except Exception as e:
                logger.exception('Failed to clean deleted records for connection %s: %s', connection_id, e)

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self.interval)
            if not self.finished.is_set():
                logger.info('Start to clean deleted records')
                try:
                    self.clean_deleted_project_records()
                except Exception as e:
                    logger.exception('Cron clean deleted project records error: %s', e)
                try:
                    self.clean_deleted_connection_records()
                except Exception as e:
                    logger.exception('Cron clean deleted connection records error: %s', e)

    def cancel(self):
        self.finished.set()
