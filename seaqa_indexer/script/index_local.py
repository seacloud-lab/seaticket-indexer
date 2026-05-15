import sys
import logging
import argparse
from typing import Optional

from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.index.index_utils import (
    update_connection_index,
    update_connection_content_vector_index,
    update_connection_summary_index,
    update_ticket_index,
    update_tickets_summary_index,
    update_knowledge_base_summary_index,
    update_knowledge_base_index
)
from seaqa_indexer.index.utils import init_logging
from seaqa_indexer.utils.seasearch_api import SeaSearchAPI
from seaqa_indexer.config import SEASEARCH_URL, SEASEARCH_TOKEN
from seaqa_indexer.index.utils import (
    gen_index_name_by_type,
    gen_kb_index_name,
    gen_ticket_index_name,
    gen_conn_summary_index_name,
    gen_kb_summary_index_name,
    gen_ticket_summary_index_name,
    gen_content_vector_index_name
)

logger = logging.getLogger(__name__)


class IndexManager:

    def __init__(self):
        self.db_session_class = init_db_session_class()
        self.seaqa_db = SeaqaDB(self.db_session_class)

    def get_connection_info(self, connection_id: str) -> tuple:
        connection = self.seaqa_db.get_project_connection_by_id(connection_id)
        return connection.id, connection.type

    def update_connection_index(self, connection_id: str):
        update_connection_index(connection_id, self.db_session_class, is_rebuild=True)
        update_connection_summary_index(connection_id, self.db_session_class, is_rebuild=True)
        update_connection_content_vector_index(connection_id, self.db_session_class, is_rebuild=True)

    def update_project_index(self, project_uuid: str):
        # update kb index
        update_knowledge_base_index(project_uuid, self.db_session_class, is_rebuild=True)
        update_knowledge_base_summary_index(project_uuid, self.db_session_class, is_rebuild=True)

        # update ticket index
        update_ticket_index(project_uuid, self.db_session_class, is_rebuild=True)
        update_tickets_summary_index(project_uuid, self.db_session_class, is_rebuild=True)

    def generate_index_names(self, connection_id: str, project_uuid: str) -> list:
        index_names = []
        if connection_id:
            connection_id, connection_type = self.get_connection_info(connection_id)
            index_names.append(gen_index_name_by_type(connection_id, connection_type))
            index_names.append(gen_conn_summary_index_name(connection_id, connection_type))
            index_names.append(gen_content_vector_index_name(connection_id, connection_type))

        elif project_uuid:
            index_names.append(gen_kb_index_name(project_uuid))
            index_names.append(gen_kb_summary_index_name(project_uuid))

            index_names.append(gen_ticket_index_name(project_uuid))
            index_names.append(gen_ticket_summary_index_name(project_uuid))

        return index_names


def start_index_local(project_uuid: str, connection_id: Optional[str]):
    """
    Start local indexing

    Args:
        project_uuid: project UUID
        connection_id: connection ID
    """
    try:
        manager = IndexManager()

        if connection_id:
            manager.update_connection_index(connection_id)
        elif project_uuid:
            manager.update_project_index(project_uuid)
        else:
            raise ValueError("Either connection_id or project_uuid must be provided")

        logger.info("Index process initialized successfully.")

    except Exception as e:
        logger.error(f"Error during index initialization: {str(e)}")
        raise


def delete_indices(project_uuid: str, connection_id: Optional[str]):
    """
    delete index

    Args:
        project_uuid: project UUID
        connection_id: connection ID
    """
    try:
        manager = IndexManager()

        index_names = manager.generate_index_names(
            connection_id, project_uuid
        )

        for index_name in index_names:
            seasearch_api = SeaSearchAPI(SEASEARCH_URL, SEASEARCH_TOKEN)
            seasearch_api.delete_index_by_name(index_name)
            logger.info(f"Successfully deleted index: {index_name}")
        else:
            logger.warning("No matching index found to delete")

    except Exception as e:
        logger.error(f"Error during index deletion: {str(e)}")
        raise


def validate_arguments(args):
    if not args.project_uuid and not args.connection_id:
        raise ValueError("Either --project_uuid or --connection_id must be provided")


def create_parser():
    parser = argparse.ArgumentParser(description='SeaQA Index Management Tool')
    subparsers = parser.add_subparsers(title='subcommands', dest='command', required=True)

    parser.add_argument(
        '--logfile',
        default=sys.stdout,
        type=argparse.FileType('a'),
        help='Log file path (default: stdout)'
    )

    parser.add_argument(
        '--loglevel',
        default='info',
        choices=['debug', 'info', 'warning', 'error'],
        help='Logging level (default: info)'
    )

    parser.add_argument(
        '--project-uuid',
        dest='project_uuid',
        help='Project UUID'
    )

    parser.add_argument(
        '--connection-id',
        dest='connection_id',
        help='Connection ID'
    )

    parser_update = subparsers.add_parser(
        'update',
        help='Update repository file index'
    )
    parser_update.set_defaults(func=start_index_local)

    parser_clear = subparsers.add_parser(
        'clear',
        help='Clear repository file index'
    )
    parser_clear.set_defaults(func=delete_indices)

    return parser


def main():
    parser = create_parser()

    if len(sys.argv) == 1:
        parser.print_help()
        return

    args = parser.parse_args()

    try:
        validate_arguments(args)
    except ValueError as e:
        print(f"Argument validation error: {e}")
        sys.exit(1)

    print(f"Project UUID: {args.project_uuid}")
    print(f"Connection ID: {args.connection_id}")

    init_logging(args)

    try:
        args.func(
            project_uuid=args.project_uuid,
            connection_id=args.connection_id
        )
    except Exception as e:
        logger.error(f"Command execution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
