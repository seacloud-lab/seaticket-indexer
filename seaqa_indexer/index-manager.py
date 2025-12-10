import argparse
import logging
import datetime
import json
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.crawler.storage import get_storage
from seaqa_indexer.crawler.utils import url_to_filename
from seaqa_indexer.mq import get_mq
from seaqa_indexer.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, LOG_LEVEL, LOG_FILE
from seaqa_indexer.app.log import LogConfigurator
from seaqa_indexer.utils import uuid_str_to_32_chars
from seaqa_indexer.utils.constants import ConnectionType
from seaqa_indexer.ai_processor.utils import generate_summary
from seaqa_indexer.seadb_models.models import (
    WebCrawlTable,
    DiscourseTopicsTable,
    GithubIssuesTable,
    SeafileTable,
    ThreadTable
)
from seaqa_indexer.seadb_models.discourse_seadb_api import DiscourseSeaDBAPI
from seaqa_indexer.seadb_models.email_seadb_api import EmailSeaDBAPI

logger = logging.getLogger(__name__)

BATCH_SIZE = 50


class IndexManager:
    def __init__(self):
        self.db_session_class = init_db_session_class()
        self.seaqa_db = SeaqaDB(self.db_session_class)
        self.seadb_api = SeaDBAPI()
        self.storage = get_storage()
        self.mq = get_mq(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)

    def submit_summary_index_task(self, connection_id):
        if not self.mq:
            logger.warning('Redis MQ is not available')
            return False

        msg = {'connection_id': connection_id}
        self.mq.lpush('summary_index_task', json.dumps(msg))
        return True

    def repair_ai_summary(self, project_uuid, connection_id=None):
        logger.info('Starting AI summary repair process...')

        if connection_id:
            connection = self.seaqa_db.get_project_connection_by_id(connection_id)
            if not connection:
                logger.error(f'Error: Connection {connection_id} not found')
                return
            if connection.project_uuid != uuid_str_to_32_chars(project_uuid):
                logger.error(f'Error: Connection {connection_id} does not belong to project {project_uuid}')
                return
            connections = [connection]
        else:
            all_connections = self.seaqa_db.get_connections_by_project_uuid(project_uuid)
            connections = []
            for conn in all_connections:
                full_conn = self.seaqa_db.get_project_connection_by_id(conn.id)
                if full_conn:
                    connections.append(full_conn)

        total_connections = len(connections)

        if total_connections == 0:
            logger.warning('No connections found matching the criteria.')
            return

        logger.info(f'Found {total_connections} connections to process.')

        processed_count = 0
        repaired_count = 0
        error_count = 0
        index_success_count = 0
        index_error_count = 0

        for connection in connections:
            processed_count += 1
            logger.info(f'[{processed_count}/{total_connections}] Processing connection ID: {connection.id}, Type: {connection.type}')

            try:
                repaired = self.repair_connection_records(connection, project_uuid)
                repaired_count += repaired

                if repaired > 0:
                    try:
                        self.submit_summary_index_task(connection.id)
                        index_success_count += 1
                        logger.info(f'Submitted index task for connection {connection.id}')
                    except Exception as e:
                        index_error_count += 1
                        logger.exception(f'Failed to submit index task for connection {connection_id}: {str(e)}')

            except Exception as e:
                error_count += 1
                logger.exception(f'Error: {str(e)}')

        logger.info('Repair process completed!')
        logger.info(f'Total connections processed: {processed_count}')
        logger.info(f'Total records repaired: {repaired_count}')
        logger.info(f'Errors encountered: {error_count}')
        logger.info(f'Index tasks submitted: {index_success_count}')
        logger.info(f'Index task errors: {index_error_count}')

    def repair_connection_records(self, connection, project_uuid):
        connection_id = connection.id
        connection_type = connection.type

        logger.info(f'Starting repair for connection {connection_id} (type: {connection_type})')

        try:
            # Use specialized repair functions based on connection type
            if connection_type == ConnectionType.SITE.value:
                repaired_count = self._repair_site_ai(connection_id, project_uuid)
            elif connection_type == ConnectionType.DISCOURSE_FORUM.value:
                repaired_count = self._repair_discourse_ai(connection_id, project_uuid)
            elif connection_type == ConnectionType.GITHUB_ISSUE.value:
                repaired_count = self._repair_github_issue_ai(connection_id, project_uuid)
            elif connection_type == ConnectionType.SEAFILE.value:
                repaired_count = self._repair_seafile_ai(connection_id, project_uuid)
            elif connection_type == ConnectionType.EMAIL.value:
                repaired_count = self._repair_email_ai(connection_id, project_uuid)
            else:
                logger.warning(f'Skipping: Unsupported connection type {connection_type}')
                return 0

            logger.info(f'Repaired {repaired_count} records for connection {connection_id}')
            return repaired_count

        except Exception as e:
            logger.exception(f'Failed to repair connection {connection_id}: {str(e)}')
            raise

    def _repair_site_ai(self, connection_id, project_uuid):
        page_size = 100
        updates = []
        ai_processing_count = 0
        table_name = WebCrawlTable.gen_table_name(connection_id)

        while True:
            sql = f"""
                SELECT _pk, url FROM `{table_name}`
                WHERE (ai_summary IS NULL OR ai_summary = '' OR ai_summary_vector IS NULL)
                ORDER BY _pk
                LIMIT {page_size}
            """
            result = self.seadb_api.query_rows(project_uuid, sql)
            rows = result.get('results', [])
            if not rows:
                break

            for row in rows:
                pk = row.get('_pk')
                url = row.get('url')
                filename = url_to_filename(url)
                file_json = self.storage.read_json(connection_id, project_uuid, filename)
                content = file_json.get('content', '')
                logger.info(f'Generating AI summary for record {pk}...')
                summary, vector = generate_summary(content, ConnectionType.SITE.value, project_uuid)
                updates.append({
                    "pk": pk,
                    "row": {
                        "ai_summary": summary,
                        "ai_summary_vector": vector,
                        "ai_processed_time": datetime.datetime.now(datetime.UTC).isoformat()
                    }
                })
                ai_processing_count += 1
                if len(updates) >= BATCH_SIZE:
                    self.seadb_api.update_rows(project_uuid, table_name, updates)
                    updates = []

            if updates:
                self.seadb_api.update_rows(project_uuid, table_name, updates)
                updates = []

        return ai_processing_count

    def _repair_discourse_ai(self, connection_id, project_uuid):
        page_size = 100
        updates = []
        table_name = DiscourseTopicsTable.gen_table_name(connection_id)
        discourse_seadb_api = DiscourseSeaDBAPI(project_uuid)
        ai_processing_count = 0

        while True:
            sql = f"""
                SELECT _pk, topic_id, title FROM `{table_name}`
                WHERE (ai_summary IS NULL OR ai_summary = '' OR ai_summary_vector IS NULL)
                ORDER BY topic_id
                LIMIT {page_size}
            """
            result = self.seadb_api.query_rows(project_uuid, sql)
            rows = result.get('results', [])
            if not rows:
                break

            for row in rows:
                pk = row.get('_pk')
                topic_id = row.get('topic_id')
                title = row.get('title', '')
                result = discourse_seadb_api.get_topic_first_reply(connection_id, topic_id)
                first_content = result.get('content', '')
                full_content = f"Title: {title}\n\nBody: {first_content}\n"
                logger.info(f'Generating AI summary for record {pk}...')
                summary, vector = generate_summary(full_content, ConnectionType.DISCOURSE_FORUM.value, project_uuid)
                updates.append({
                    "pk": pk,
                    "row": {
                        "ai_summary": summary,
                        "ai_summary_vector": vector,
                        "ai_processed_time": datetime.datetime.now(datetime.UTC).isoformat()
                    }
                })
                ai_processing_count += 1
                if len(updates) >= BATCH_SIZE:
                    self.seadb_api.update_rows(project_uuid, table_name, updates)
                    updates = []

            if updates:
                self.seadb_api.update_rows(project_uuid, table_name, updates)

        return ai_processing_count

    def _repair_github_issue_ai(self, connection_id, project_uuid):
        page_size = 100
        updates = []
        table_name = GithubIssuesTable.gen_table_name(connection_id)
        ai_processing_count = 0

        while True:
            sql = f"""
                SELECT * FROM `{table_name}`
                WHERE (ai_summary IS NULL OR ai_summary = '' OR ai_summary_vector IS NULL)
                ORDER BY issue_id
                LIMIT {page_size}
            """
            result = self.seadb_api.query_rows(project_uuid, sql)
            rows = result.get('results', [])
            if not rows:
                break

            for row in rows:
                pk = row.get('_pk')
                title = row.get('title', '')
                content = row.get('content', '')
                full_content = f"Title: {title}\n\nBody: {content}\n"
                logger.info(f'Generating AI summary for record {pk}...')
                summary, vector = generate_summary(full_content, ConnectionType.GITHUB_ISSUE.value, project_uuid)
                updates.append({
                    "pk": pk,
                    "row": {
                        "ai_summary": summary,
                        "ai_summary_vector": vector,
                        "ai_processed_time": datetime.datetime.now(datetime.UTC).isoformat()
                    }
                })
                ai_processing_count += 1
                if len(updates) >= BATCH_SIZE:
                    self.seadb_api.update_rows(project_uuid, table_name, updates)
                    updates = []

            if updates:
                self.seadb_api.update_rows(project_uuid, table_name, updates)

        return ai_processing_count

    def _repair_seafile_ai(self, connection_id, project_uuid):
        page_size = 100
        updates = []
        table_name = SeafileTable.gen_table_name(connection_id)
        ai_processing_count = 0

        while True:
            sql = f"""
                SELECT * FROM `{table_name}`
                WHERE (ai_summary IS NULL OR ai_summary = '')
                ORDER BY path, title
                LIMIT {page_size}
            """
            result = self.seadb_api.query_rows(project_uuid, sql)
            rows = result.get('results', [])
            if not rows:
                break
            for row in rows:
                pk = row.get('_pk')
                title = row.get('title')
                content = row.get('content')
                summary = 'empty doc'
                vector = []
                if content:
                    full_content = f"Title: {title}\n\nContent: {content}\n"
                    logger.info(f'Generating AI summary for record {pk}...')
                    summary, vector = generate_summary(full_content, ConnectionType.SEAFILE.value, project_uuid)
                updates.append({
                    "pk": pk,
                    "row": {
                        "ai_summary": summary,
                        "ai_summary_vector": vector,
                        "ai_processed_time": datetime.datetime.now(datetime.UTC).isoformat()
                    }
                })
                ai_processing_count += 1
                if len(updates) >= BATCH_SIZE:
                    self.seadb_api.update_rows(project_uuid, table_name, updates)
                    updates = []

            if updates:
                self.seadb_api.update_rows(project_uuid, table_name, updates)

        return ai_processing_count

    def _repair_email_ai(self, connection_id, project_uuid):
        page_size = 100
        updates = []
        table_name = ThreadTable.gen_table_name(connection_id)
        email_seadb_api = EmailSeaDBAPI(project_uuid)
        ai_processing_count = 0

        while True:
            sql = f"""
                SELECT * FROM `{table_name}`
                WHERE (ai_summary IS NULL OR ai_summary = '' OR ai_summary_vector IS NULL)
                LIMIT {page_size}
            """
            result = self.seadb_api.query_rows(project_uuid, sql)
            rows = result.get('results', [])
            if not rows:
                break

            for row in rows:
                pk = row.get('_pk')
                title = row.get('title', '')
                result = email_seadb_api.get_first_email(connection_id, pk)
                content = result.get('content', '')
                full_content = f"Title: {title}\n\nBody: {content}\n"
                logger.info(f'Generating AI summary for record {pk}...')
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
                if len(updates) >= BATCH_SIZE:
                    self.seadb_api.update_rows(project_uuid, table_name, updates)
                    updates = []

            if updates:
                self.seadb_api.update_rows(project_uuid, table_name, updates)

        return ai_processing_count


def main():
    LogConfigurator(LOG_LEVEL)

    parser = argparse.ArgumentParser(
        description='Index Manager - Manage AI summary and vector indexing',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Repair all connections in a project
  python index-manager.py repair_ai_summary abc123def456

  # Repair a specific connection
  python index-manager.py repair_ai_summary abc123def456 --connection-id 42
        """
    )

    parser.add_argument(
        'action',
        choices=['repair_ai_summary'],
        help='Action to perform'
    )

    parser.add_argument(
        'project_uuid',
        type=str,
        help='Project UUID (required)'
    )

    parser.add_argument(
        '--connection-id',
        type=int,
        help='Only repair records for a specific connection (optional)'
    )

    args = parser.parse_args()
    manager = IndexManager()

    if args.action == 'repair_ai_summary':
        manager.repair_ai_summary(args.project_uuid, args.connection_id)
    else:
        logger.info(f'Unknown action: {args.action}')
        sys.exit(1)


if __name__ == '__main__':
    main()
