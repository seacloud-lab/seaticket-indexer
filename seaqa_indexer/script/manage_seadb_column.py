import sys
import logging
import argparse
import requests
import base64
from urllib.parse import quote
import fnmatch

from sqlalchemy import text

from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.index.utils import init_logging

from seaqa_indexer.seadb_models.models import PropertyTypes, ListTypes
from seaqa_indexer.config import SEADB_SERVER_URL, SEADB_USER, SEADB_PASSWORD
from seaqa_indexer.utils.constants import TEMPLATE_NAME


logger = logging.getLogger(__name__)


COLUMN_TYPE_TO_DATA_MAPPING = {
    PropertyTypes.TEXT: {
        'text_compressed': {'compressed': True}
    },
    PropertyTypes.LIST: {
        'list_vector': ListTypes.vector,
        'list_int': ListTypes.int,
    }
}

TABLE_CONTAINING_SUB_TABLE_NAMES = ['email', 'github_issue', 'discourse_forum', 'linear_issue', 'discord_thread', 'jira_issue']
SUB_TABLE_NAMES = ['email_thread', 'github_issue_comments', 'discourse_forum_replies', 'linear_issue_comments', 'discord_thread_messages', 'jira_issue_comments']


def parse_response(response):
    if response.status_code >= 400 or response.status_code < 200:
        raise ConnectionError(response.status_code, response.text)
    else:
        try:
            return response.json()
        except:
            return {}

class SeaDBAPI:
    def __init__(self, timeout=30):
        self.timeout = timeout
        self.server_url = SEADB_SERVER_URL
        self.headers = None
        self.gen_headers()

    def gen_headers(self):
        auth_str = f"{SEADB_USER}:{SEADB_PASSWORD}"
        b64 = base64.b64encode(auth_str.encode("utf-8")).decode()
        self.headers = {
            "Authorization": f"Basic {b64}"
        }

    # template base
    def get_base_metadata(self, template_name):
        url = f'{self.server_url}/api/v1/{quote(template_name)}/metadata'
        response = requests.get(url, headers=self.headers, timeout=self.timeout)
        return parse_response(response)

    # columns
    def add_column(self, template_name, table_id, column):
        url = f'{self.server_url}/api/v1/{quote(template_name)}/columns'
        data = {
            'table_id': table_id,
            'column_name': column['column_name'],
            'column_type': column['column_type'],
        }
        if column.get('column_data'):
            data['column_data'] = column['column_data']
        response = requests.post(url, json=data, headers=self.headers, timeout=self.timeout)
        return parse_response(response)

    def delete_column(self, template_name, table_id, column_key):
        url = f'{self.server_url}/api/v1/{quote(template_name)}/columns'
        data = {
            'table_id': table_id,
            'column_key': column_key,
        }
        response = requests.delete(url, json=data, headers=self.headers, timeout=self.timeout)
        return parse_response(response)

    def update_column(self, template_name, table_id, column_key, new_column_name=None, update_column_data=None):
        url = f'{self.server_url}/api/v1/{quote(template_name)}/columns'
        data = {
            'table_id': table_id,
            'column_key': column_key
        }
        if new_column_name:
            data['new_column_name'] = new_column_name
        if update_column_data:
            data['update_column_data'] = update_column_data
        response = requests.put(url, json=data, headers=self.headers, timeout=self.timeout)
        return parse_response(response)

    def create_column_index(self, template_name, table_id, columns):
        url = f'{self.server_url}/api/v1/{quote(template_name)}/index'
        data = {
            'table_id': table_id,
            'columns': columns,
        }
        response = requests.post(url, json=data, headers=self.headers, timeout=self.timeout)
        return parse_response(response)

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
    
    def is_this_table(self, seadb_table_name, table_name):
        # Wildcard matching, e.g. linear_issue_* matches linear_issue_123.
        if '*' in table_name:
            if not fnmatch.fnmatchcase(seadb_table_name, table_name):
                return False
            # Do not touch sub tables, e.g. linear_issue_* must not match linear_issue_comments_123
            main_table_name = table_name.split('*', 1)[0].rstrip('_')
            if main_table_name in TABLE_CONTAINING_SUB_TABLE_NAMES and \
                any(keyword in seadb_table_name for keyword in SUB_TABLE_NAMES):
                return False
            return True
        return seadb_table_name == table_name
    
    def add_seadb_column(self, table_name, column_name, column_type, column_data_name, need_add_index=False):
        column_data = self.get_column_data(column_data_name, column_type)

        template_base_info = self.seadb_api.get_base_metadata(TEMPLATE_NAME)

        tables = template_base_info.get('tables')
        try:
            for table in tables: # type: ignore
                seadb_table_name = table['name']
                table_id = table.get('id')
                if self.is_this_table(seadb_table_name, table_name):
                    mapped_column = {
                        'column_name': column_name,
                        'column_type': column_type
                    }
                    if column_data:
                        mapped_column['column_data'] = column_data
                    self.seadb_api.add_column(TEMPLATE_NAME, table_id, mapped_column)
                    if need_add_index:
                        self.seadb_api.create_column_index(
                            TEMPLATE_NAME,
                            table_id,
                            [
                                column_name,
                            ]
                        )
                    
                    logger.info('Successfully add column %s for project %s table %s', column_name, TEMPLATE_NAME, seadb_table_name)
        except Exception as e:
            logger.error("template base:%s fail to add column %s for table %s, error: %s", TEMPLATE_NAME, column_name, table_name, str(e))

    def delete_seadb_column(self, table_name, column_name):
        template_info = self.seadb_api.get_template_info(TEMPLATE_NAME)

        tables = template_info.get('tables')
        try:
            for table in tables: # type: ignore
                seadb_table_name = table['name']
                table_id = table.get('id')
                columns = table.get('columns')
                if self.is_this_table(seadb_table_name, table_name):
                    column = next((column for column in columns if column['name'] == column_name), None)
                    if not column:
                        logger.warning('project %s table_name %s does not found %s column', TEMPLATE_NAME, table_name, column_name)
                        continue
                    column_key = column.get('key')
                    self.seadb_api.delete_column(TEMPLATE_NAME, table_id, column_key)
                    logger.info('Successfully delete column %s for project %s table %s', column_name, TEMPLATE_NAME, seadb_table_name)
        except Exception as e:
            logger.error("project_uuid:%s fail to add column %s for table %s, error: %s", TEMPLATE_NAME, column_name, table_name, str(e))

    def update_seadb_column_format(self, table_name, column_name, column_format):
        template_info = self.seadb_api.get_template_info(TEMPLATE_NAME)
        tables = template_info.get('tables')

        try:
            for table in tables: # type: ignore
                seadb_table_name = table['name']
                if not self.is_this_table(seadb_table_name, table_name):
                    continue

                column = next(
                    (column for column in table.get('columns') or [] if column['name'] == column_name),
                    None
                )
                if not column:
                    logger.warning(
                        'project %s table_name %s does not have column %s',
                        TEMPLATE_NAME, seadb_table_name, column_name
                    )
                    continue
                if (column.get('data') or {}).get('format') == column_format:
                    continue

                self.seadb_api.update_column(
                    TEMPLATE_NAME,
                    table['id'],
                    column['key'],
                    update_column_data={'format': column_format},
                )
                logger.info(
                    'Successfully updated column %s format for project %s table %s',
                    column_name, TEMPLATE_NAME, seadb_table_name
                )
        except Exception as e:
            logger.error(
                'project_uuid:%s fail to update column %s format for table %s, error: %s',
                TEMPLATE_NAME, column_name, table_name, str(e)
            )


def start_add_column(args):
    table_name = args.table_name
    column_name = args.column_name
    column_type = args.column_type
    column_data_name = args.column_data_name
    need_add_index = args.need_add_index
    logger.info('start to add column %s for table %s, column_type: %s, column_data_name: %s', column_name, table_name, column_type, column_data_name)
    try:
        manager = ColumnManager()
        manager.add_seadb_column(table_name, column_name, column_type, column_data_name, need_add_index)
        logger.info("table %s add new column %s successfully.", table_name, column_name)

    except Exception as e:
        logger.error(f"Error during add column: {str(e)}")
        raise


def start_delete_column(args):
    table_name = args.table_name
    column_name = args.column_name
    try:
        manager = ColumnManager()
        manager.delete_seadb_column(
            table_name, column_name
        )
        logger.info(f"Successfully deleted column")

    except Exception as e:
        logger.error(f"Error during delete column: {str(e)}")
        raise


def start_update_column_format(args):
    try:
        manager = ColumnManager()
        manager.update_seadb_column_format(
            args.table_name, args.column_name, args.column_format
        )
        logger.info('Successfully updated column format')
    except Exception as e:
        logger.error(f'Error during update column format: {str(e)}')
        raise


def validate_arguments(args):
    if not args.table_name or not args.column_name:
        raise ValueError('--table-name and --column-name must be provided')
    if args.command == 'update-column-format' and not args.column_format:
        raise ValueError('--format must be provided')

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
    parent_parser = argparse.ArgumentParser(add_help=False)

    parent_parser.add_argument(
        '--logfile',
        default=sys.stdout,
        type=argparse.FileType('a'),
        help='Log file path (default: stdout)'
    )

    parent_parser.add_argument(
        '--loglevel',
        default='info',
        choices=['debug', 'info', 'warning', 'error'],
        help='Logging level (default: info)'
    )

    parent_parser.add_argument(
        '--table-name',
        dest='table_name',
        help='Table Name'
    )

    parent_parser.add_argument(
        '--column-name',
        dest='column_name',
        help='column name'
    )

    parent_parser.add_argument(
        '--column-type',
        dest='column_type',
        choices=[value for key, value in PropertyTypes.__dict__.items() if not key.startswith('__')],
        help='column type'
    )

    parent_parser.add_argument(
        '--column-data-name',
        dest='column_data_name',
        choices=['text_compresse', 'list_float32', 'list_float64', 'list_int', 'list_bool', 'list_datetime'],
        help='column data name'
    )

    parent_parser.add_argument(
        '--need-add-index',
        type=str_to_bool,
        dest='need_add_index',
        default=False,
        help='Need add index'
    )

    parent_parser.add_argument(
        '--format',
        dest='column_format',
        help='column date format'
    )

    parser = argparse.ArgumentParser(description='SeaQA Index Management Tool')
    subparsers = parser.add_subparsers(title='subcommands', dest='command', required=True)

    parser_add = subparsers.add_parser(
        'add-column',
        parents=[parent_parser],
        help='add seadb column'
    )
    parser_add.set_defaults(func=start_add_column)

    parser_delete = subparsers.add_parser(
        'delete-column',
        parents=[parent_parser],
        help='delete seadb column'
    )
    parser_delete.set_defaults(func=start_delete_column)

    parser_update_format = subparsers.add_parser(
        'update-column-format',
        parents=[parent_parser],
        help='update seadb column format'
    )
    parser_update_format.set_defaults(func=start_update_column_format)

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

    init_logging(args)

    try:
        args.func(args)
    except Exception as e:
        logger.error(f"Command execution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
