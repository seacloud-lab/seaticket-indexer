import sys
import logging
import argparse

from sqlalchemy import text

from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.index.utils import init_logging

from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.seadb_models.models import PropertyTypes, ListTypes

logger = logging.getLogger(__name__)

class TableType:
    PROJECT = 'project'
    CONNECTION = 'connection'


COLUMN_TYPE_TO_DATA_MAPPING = {
    PropertyTypes.TEXT: {
        'text_compresse': {'compressed': True}
    },
    PropertyTypes.LIST: {
        'list_vector': ListTypes.vector,
        'list_int': ListTypes.int,
    }
}

TABLE_CONTAINING_SUB_TABLE_NAMES = ['email', 'github_issue', 'discourse_forum']
SUB_TABLE_NAMES = ['email_thread', 'github_issue_comments', 'discourse_forum_replies']

class ColumnManager:

    def __init__(self):
        self.db_session_class = init_db_session_class()
        self.seaqa_db = SeaqaDB(self.db_session_class)
        self.seadb_api = SeaDBAPI()
    
    def get_projects_by_page(self, limit, start):
        with self.db_session_class() as session:
            sql = """SELECT uuid FROM projects ORDER BY id LIMIT :limit offset :start"""
            projects = session.execute(text(sql), {'limit':limit, 'start': start}).fetchall()
            return projects
    
    def get_column_data(self, column_data_name, column_type):
        column_data = None
        if not column_data_name:
            return column_data
        column_data = COLUMN_TYPE_TO_DATA_MAPPING.get(column_type, {}).get(column_data_name)
        if not column_data:
            raise Exception('column_data_name %s is unavailable' % column_data_name)
        return column_data
    
    def is_this_table(self, seadb_table_name, table_name, table_type):
        # Check if the table is the target table
        if table_type == TableType.PROJECT and seadb_table_name == table_name:
            return True
        if table_type == TableType.CONNECTION and table_name in seadb_table_name:
            if table_name in TABLE_CONTAINING_SUB_TABLE_NAMES and \
                any(keyword in seadb_table_name for keyword in SUB_TABLE_NAMES):
                # only add column for main table
                return False
            else:
                return True
    
    def add_seadb_column(self, table_name, table_type, column_name, column_type, column_data_name, need_add_index=False):
        column_data = self.get_column_data(column_data_name, column_type)
        limit = 1000
        start = 0
        while True:
            projects = self.get_projects_by_page(limit, start)

            for project in projects:
                project_uuid = project.uuid
                logger.info('start add column for project %s', project_uuid)
                try:
                    base_info = self.seadb_api.get_base_metadata(project_uuid)
                    
                    tables = base_info['tables']
                    for table in tables:
                        seadb_table_name = table['name']
                        table_id = table.get('id')
                        if self.is_this_table(seadb_table_name, table_name, table_type):
                            mapped_column = {
                                'column_name': column_name,
                                'column_type': column_type
                            }
                            if column_data:
                                mapped_column['column_data'] = column_data
                            self.seadb_api.add_column(project_uuid, table_id, mapped_column)
                            if need_add_index:
                                self.seadb_api.create_column_index(
                                    project_uuid,
                                    table_id,
                                    [
                                        column_name,
                                    ]
                                )
                            
                            logger.info('Successfully add column %s for project %s table %s', column_name, project_uuid, seadb_table_name)
                except Exception as e:
                    logger.error("project_uuid:%s fail to add column %s for table %s, error: %s", project_uuid, column_name, table_name, str(e))
            
            start += limit

            if len(projects) < limit:
                break
    
    def delete_seadb_column(self, table_name, table_type, column_name):
        limit = 1000
        start = 0
        while True:
            projects = self.get_projects_by_page(limit, start)

            for project in projects:
                project_uuid = project.uuid
                logger.info('start delete column %s for project %s table_name %s', column_name, project_uuid, table_name)
                try:
                    try:
                        base_info = self.seadb_api.get_base_metadata(project_uuid)
                    except Exception as e:
                        logger.error('project %s fail to add column: %s to table name: %s, error: %s', project_uuid, column_name, table_name, e)
                        continue

                    tables = base_info['tables']
                    for table in tables:
                        seadb_table_name = table['name']
                        table_id = table.get('id')
                        columns = table.get('columns')
                        if self.is_this_table(seadb_table_name, table_name, table_type):
                            column = next((column for column in columns if column['name'] == column_name), None)
                            if not column:
                                logger.warning('project %s table_name %s does not found %s column', project_uuid, table_name, column_name)
                                continue
                            column_key = column.get('key')
                            self.seadb_api.delete_column(project_uuid, table_id, column_key)
                            logger.info('Successfully delete column %s for project %s table %s', column_name, project_uuid, seadb_table_name)
                except Exception as e:
                    logger.error("project_uuid:%s fail to add column %s for table %s, error: %s", project_uuid, column_name, table_name, str(e))
            
            start += limit

            if len(projects) < limit:
                break


def start_add_column(args):
    table_name = args.table_name
    table_type = args.table_type
    column_name = args.column_name
    column_type = args.column_type
    column_data_name = args.column_data_name
    need_add_index = args.need_add_index
    logger.info('start to add column %s for table %s, table_type: %s, column_type: %s, column_data_name: %s', column_name, table_name, table_type, column_type, column_data_name)
    try:
        manager = ColumnManager()
        manager.add_seadb_column(table_name, table_type, column_name, column_type, column_data_name, need_add_index)
        logger.info("table %s add new column %s successfully.", table_name, column_name)

    except Exception as e:
        logger.error(f"Error during add column: {str(e)}")
        raise


def start_delete_column(args):
    table_name = args.table_name
    table_type = args.table_type
    column_name = args.column_name
    try:
        manager = ColumnManager()
        manager.delete_seadb_column(
            table_name, table_type, column_name
        )
        logger.info(f"Successfully deleted column")

    except Exception as e:
        logger.error(f"Error during delete column: {str(e)}")
        raise


def validate_arguments(args):
    if not args.table_name or not args.table_type or not args.column_name:
        raise ValueError("Either --tabe-name and --table-type and --column-name must be provided")

def str_to_bool(v):
    if isinstance(v, bool):
       return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError(f'Boolean value expected, got: {v}')


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
        '--table-name',
        dest='table_name',
        help='Table Name'
    )

    parser.add_argument(
        '--table-type',
        dest='table_type',
        choices=[TableType.PROJECT, TableType.CONNECTION],
        help='Table type'
    )

    parser.add_argument(
        '--column-name',
        dest='column_name',
        help='column name'
    )

    parser.add_argument(
        '--column-type',
        dest='column_type',
        choices=[value for key, value in PropertyTypes.__dict__.items() if not key.startswith('__')],
        help='column type'
    )

    parser.add_argument(
        '--column-data-name',
        dest='column_data_name',
        choices=['text_compresse', 'list_float32', 'list_float64', 'list_int', 'list_bool', 'list_datetime'],
        help='column data name'
    )

    parser.add_argument(
        '--need-add-index',
        type=str_to_bool,
        dest='need_add_index',
        default=False,
        help='Need add index'
    )

    parser_add = subparsers.add_parser(
        'add',
        help='add seadb column'
    )
    parser_add.set_defaults(func=start_add_column)

    parser_clear = subparsers.add_parser(
        'delete',
        help='delete seadb column'
    )
    parser_clear.set_defaults(func=start_delete_column)

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

    print(f"Table Name: {args.table_name}")
    print(f"Column Name: {args.column_name}")
    print(f"Table Type: {args.table_type}")

    init_logging(args)

    try:
        args.func(args)
    except Exception as e:
        logger.error(f"Command execution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
