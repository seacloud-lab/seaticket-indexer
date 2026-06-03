import datetime
import json

from seaqa_indexer.seadb_models.utils import get_table_name_from_schema, get_column_name_from_schema
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.config import GENERAL_TASK_COLUMN_MAPPINGS

from seaqa_indexer.seadb_models.models import SchemaTableNames



class GeneralTaskSeaDBAPI:
    def __init__(self, base_id):
        self.base_id = base_id
        self.seadb_api = SeaDBAPI()

    def _table_name(self, connection_id):
        return get_table_name_from_schema(SchemaTableNames.GENERAL_TASK, connection_id)

    def _task_user_table_name(self, connection_id):
        return get_table_name_from_schema(SchemaTableNames.GENERAL_TASK_USER, connection_id)

    def get_need_ai_tasks_by_page(self, connection_id, page_size):
        table_name = self._table_name(connection_id)
        sql = f"""
            SELECT _pk, title, status, size, priority, assignees, participants, others, content, modified_time, due_date, created_time
            FROM `{table_name}`
            WHERE (ai_processed_time IS NULL OR ai_processed_time < sync_time)
              AND (deleted IS NULL OR deleted = false)
            LIMIT {page_size}
        """
        return self.seadb_api.query_rows(self.base_id, sql).get('results', [])

    def update_rows(self, connection_id, updates):
        if not updates:
            return
        table_name = self._table_name(connection_id)
        self.seadb_api.update_rows(self.base_id, table_name, updates)

    def get_tasks_by_page(self, connection_id, since, page_size, offset):
        table_name = self._table_name(connection_id)
        since_filter = ''
        if since:
            since_filter = f"WHERE record_modified_time >= '{since}'"
        sql = f"""
            SELECT _pk, title, content, modified_time, deleted, outdated
            FROM `{table_name}`
            {since_filter}
            LIMIT {page_size} OFFSET {offset}
        """
        return self.seadb_api.query_rows(self.base_id, sql).get('results', [])

    def get_pk_by_source_task_id(self, connection_id, source_task_id):
        table_name = self._table_name(connection_id)
        escaped = source_task_id.replace("'", "''")
        sql = f"SELECT _pk FROM `{table_name}` WHERE source_task_id = '{escaped}' LIMIT 1"
        results = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
        if not results:
            return None
        return results[0].get('_pk')

    def get_rows_by_source_task_ids(self, connection_id, source_task_ids):
        table_name = self._table_name(connection_id)
        escaped_ids = "', '".join(source_task_id.replace("'", "''") for source_task_id in source_task_ids)
        sql = (
            f"SELECT _pk, source_task_id, modified_time, deleted "
            f"FROM `{table_name}` "
            f"WHERE source_task_id IN ('{escaped_ids}')"
        )
        results = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
        return {row.get('source_task_id'): row for row in results }

    def get_existing_user_mapping_pks(self, connection_id, emails):
        if not emails:
            return {}
        table_name = self._task_user_table_name(connection_id)
        escaped_emails = "', '".join(e.replace("'", "''") for e in emails)
        sql = f"SELECT email, _pk FROM `{table_name}` WHERE email IN ('{escaped_emails}')"
        results = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
        return {row.get('email'): row.get('_pk') for row in results}

    def append_rows(self, connection_id, rows):
        if not rows:
            return
        table_name = self._table_name(connection_id)
        self.seadb_api.insert_rows(self.base_id, table_name, rows)

    def upsert_user_mappings(self, connection_id, user_mappings):
        if not user_mappings:
            return
        now = datetime.datetime.now(datetime.UTC).isoformat()
        emails = [str(user_mapping.get('email') or '').strip() for user_mapping in user_mappings]
        emails = [e for e in emails if e]
        existing_pks = self.get_existing_user_mapping_pks(connection_id, emails)

        inserts = []
        updates = []
        for user_mapping in user_mappings:
            email = str(user_mapping.get('email') or '').strip()
            name = str(user_mapping.get('name') or '').strip()
            if not email or not name:
                continue
            row = {
                get_column_name_from_schema(SchemaTableNames.GENERAL_TASK_USER, 'email'): email,
                get_column_name_from_schema(SchemaTableNames.GENERAL_TASK_USER, 'name'): name,
                get_column_name_from_schema(SchemaTableNames.GENERAL_TASK_USER, 'record_modified_time'): now,
            }
            existing_pk = existing_pks.get(email)
            if existing_pk:
                updates.append({'pk': existing_pk, 'row': row})
            else:
                inserts.append(row)

        table_name = self._task_user_table_name(connection_id)
        if inserts:
            self.seadb_api.insert_rows(self.base_id, table_name, inserts)
        if updates:
            self.seadb_api.update_rows(self.base_id, table_name, updates)

    def ensure_task_column_options(self, connection_id, tasks):
        if not tasks:
            return
        table_name = self._table_name(connection_id)
        metadata = self.seadb_api.get_base_metadata(self.base_id)
        table_info = {}
        for table in metadata.get('tables', []):
            if table.get('name') == table_name:
                table_info = table
                break
        if not table_info:
            return

        table_id = table_info.get('id')
        columns = table_info.get('columns', []) or []
        column_name_to_meta = {column.get('name'): column for column in columns}
        target_columns = [
            get_column_name_from_schema(SchemaTableNames.GENERAL_TASK, 'status'),
            get_column_name_from_schema(SchemaTableNames.GENERAL_TASK, 'size'),
            get_column_name_from_schema(SchemaTableNames.GENERAL_TASK, 'priority'),
        ]
        desired_values = {column_name: set() for column_name in target_columns}
        for task in tasks:
            status = task.get('status')
            size = task.get('size')
            priority = task.get('priority')
            if status:
                desired_values[get_column_name_from_schema(SchemaTableNames.GENERAL_TASK, 'status')].add(str(status))
            if size:
                desired_values[get_column_name_from_schema(SchemaTableNames.GENERAL_TASK, 'size')].add(str(size))
            if priority:
                desired_values[get_column_name_from_schema(SchemaTableNames.GENERAL_TASK, 'priority')].add(str(priority))
        for column_name in target_columns:
            column_meta = column_name_to_meta.get(column_name)
            if not column_meta:
                continue
            column_key = column_meta.get('key')
            old_option_names = set()
            data = column_meta.get('data') or {}
            options = data.get('options', []) or []
            for option in options:
                option_name = option.get('name')
                if option_name:
                    old_option_names.add(option_name)

            new_option_names = desired_values.get(column_name, set()) - old_option_names
            for option_name in new_option_names:
                option_data_payload = {}
                field_option_meta = GENERAL_TASK_COLUMN_MAPPINGS.get(column_name) or {}
                option_mapping = field_option_meta.get(str(option_name))
                if option_mapping:
                    option_data_payload['color'] = option_mapping.get('color')
                    option_data_payload['text_color'] = option_mapping.get('text_color')
                option_data = {
                    'table_id': table_id,
                    'column_key': column_key,
                    'option_name': option_name,
                }
                if option_data_payload:
                    option_data['option_data'] = option_data_payload
                self.seadb_api.add_column_option(self.base_id, option_data)

    def build_row_data(self, task):
        content = task.get('description', '')
        others = task.get('others')
        if isinstance(others, (dict, list)):
            others = json.dumps(others, ensure_ascii=False)
        elif others is None:
            others = ''
        else:
            others = str(others)

        now = datetime.datetime.now(datetime.UTC).isoformat()
        return {
            get_column_name_from_schema(SchemaTableNames.GENERAL_TASK, 'source_task_id'): str(task.get('task_id') or '').strip(),
            get_column_name_from_schema(SchemaTableNames.GENERAL_TASK, 'url'): str(task.get('url') or '').strip(),
            get_column_name_from_schema(SchemaTableNames.GENERAL_TASK, 'title'): task.get('title', ''),
            get_column_name_from_schema(SchemaTableNames.GENERAL_TASK, 'status'): task.get('status'),
            get_column_name_from_schema(SchemaTableNames.GENERAL_TASK, 'size'): task.get('size'),
            get_column_name_from_schema(SchemaTableNames.GENERAL_TASK, 'priority'): task.get('priority'),
            get_column_name_from_schema(SchemaTableNames.GENERAL_TASK, 'assignees'): task.get('assignees', []),
            get_column_name_from_schema(SchemaTableNames.GENERAL_TASK, 'participants'): task.get('participants', []),
            get_column_name_from_schema(SchemaTableNames.GENERAL_TASK, 'version'): task.get('version', ''),
            get_column_name_from_schema(SchemaTableNames.GENERAL_TASK, 'others'): others,
            get_column_name_from_schema(SchemaTableNames.GENERAL_TASK, 'content'): content,
            get_column_name_from_schema(SchemaTableNames.GENERAL_TASK, 'due_date'): task.get('due_date'),
            get_column_name_from_schema(SchemaTableNames.GENERAL_TASK, 'modified_time'): task.get('modified_time') or now,
            get_column_name_from_schema(SchemaTableNames.GENERAL_TASK, 'created_time'): task.get('created_time') or now,
            get_column_name_from_schema(SchemaTableNames.GENERAL_TASK, 'sync_time'): datetime.datetime.now(datetime.UTC).isoformat(),
            get_column_name_from_schema(SchemaTableNames.GENERAL_TASK, 'record_modified_time'): now,
            get_column_name_from_schema(SchemaTableNames.GENERAL_TASK, 'deleted'): bool(task.get('deleted', False)),
        }

    def get_total_count(self, connection_id):
        table_name = self._table_name(connection_id)
        sql = f"SELECT COUNT(*) as total FROM `{table_name}` WHERE deleted = false OR deleted IS NULL"
        results = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
        return int(results[0].get('total', 0)) if results else 0
