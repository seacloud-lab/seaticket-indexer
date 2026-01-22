import argparse
import logging
import sys
import os
import json

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.mq import get_mq
from seaqa_indexer.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
from seaqa_indexer.utils import uuid_str_to_32_chars
from seaqa_indexer.handlers import get_handler_or_none

logger = logging.getLogger(__name__)

BATCH_SIZE = 50


class IndexManager:
    def __init__(self):
        self.db_session_class = init_db_session_class()
        self.seaqa_db = SeaqaDB(self.db_session_class)
        self.seadb_api = SeaDBAPI()
        self.mq = get_mq(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)

    def repair_ai_summary(self, project_uuid, connection_id=None):
        logger.info('Starting AI summary repair process...')
        project_uuid = uuid_str_to_32_chars(project_uuid)
        if connection_id:
            connection = self.seaqa_db.get_project_connection_by_id(connection_id)
            if not connection:
                logger.error(f'Error: Connection {connection_id} not found')
                return
            if connection.project_uuid != project_uuid:
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

        for connection in connections:
            processed_count += 1
            logger.info(f'[{processed_count}/{total_connections}] Processing connection ID: {connection.id}, Type: {connection.type}')

            try:
                repaired = self.repair_connection_records(connection, project_uuid)
                repaired_count += repaired
            except Exception as e:
                logger.exception(f'Error: {str(e)}')

        logger.info('Repair process completed!')
        logger.info(f'Total connections processed: {processed_count}')
        logger.info(f'Total records queued for repair: {repaired_count}')

    def _clear_ai_processed_time(self, connection_id, connection_type, project_uuid):

        conn_handler = get_handler_or_none(connection_type)
        if not conn_handler:
            logger.warning(f'Unsupported connection type: {connection_type}')
            return 0
        
        table_name = conn_handler.gen_table_name(connection_id)

        page_size = 100
        offset = 0
        cleared_count = 0
        while True:
            sql = f"""
                SELECT _pk FROM `{table_name}`
                WHERE (ai_summary IS NULL OR ai_summary = '' OR ai_summary_vector IS NULL) 
                AND (deleted = false OR deleted IS NULL) LIMIT {page_size} OFFSET {offset}
            """
            result = self.seadb_api.query_rows(project_uuid, sql)
            rows = result.get('results', [])
            offset += page_size

            if not rows:
                logger.info(f'No records need repair for connection {connection_id}')
                break

            cleared_count += len(rows)
            updates = []
            for row in rows:
                pk = row.get('_pk')
                updates.append({
                    "pk": pk,
                    "row": {
                        "ai_processed_time": None
                    }
                })

            self.seadb_api.update_rows(project_uuid, table_name, updates)

        logger.info(f'Cleared ai_processed_time for {cleared_count} records in connection {connection_id}')
        return cleared_count

    def repair_connection_records(self, connection, project_uuid):
        connection_id = connection.id
        connection_type = connection.type

        logger.info(f'Starting repair for connection {connection_id} (type: {connection_type})')

        try:
            repaired_count = self._clear_ai_processed_time(connection_id, connection_type, project_uuid)

            if repaired_count > 0:
                logger.info(f'Submitting AI process task for connection {connection_id}')
                msg_content = {
                    'connection_id': connection_id,
                }
                self.mq.lpush('ai_process_task', json.dumps(msg_content))

            logger.info(f'{repaired_count} records will be repaired for connection {connection_id}')
            return repaired_count

        except Exception as e:
            logger.exception(f'Failed to repair connection {connection_id}: {str(e)}')
            raise


def main():
    logging.basicConfig(
        format='[seaqa_indexer] [%(asctime)s] [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level='INFO',
        stream=sys.stdout
    )

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
