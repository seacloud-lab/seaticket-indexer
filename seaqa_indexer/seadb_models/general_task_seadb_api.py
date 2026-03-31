from seaqa_indexer.seadb_models.models import GeneralTaskTable
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.config import GENERAL_TASK_COLUMN_MAPPINGS


class GeneralTaskSeaDBAPI:
    def __init__(self, base_id):
        self.base_id = base_id
        self.seadb_api = SeaDBAPI()

    def _table_name(self, connection_id):
        return GeneralTaskTable.gen_table_name(connection_id)

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

    def get_rows_by_page(self, connection_id, page_size, offset):
        table_name = self._table_name(connection_id)
        sql = f"""
            SELECT _pk, title, content, connection_id 
            FROM `{table_name}`
            LIMIT {page_size} OFFSET {offset}
        """
        return self.seadb_api.query_rows(self.base_id, sql).get('results', [])

    def get_pk_by_source_row_id(self, connection_id, source_row_id):
        table_name = self._table_name(connection_id)
        escaped = source_row_id.replace("'", "''")
        sql = f"SELECT _pk FROM `{table_name}` WHERE source_row_id = '{escaped}' LIMIT 1"
        results = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
        if not results:
            return None
        return results[0].get('_pk')

    def append_rows(self, connection_id, rows):
        if not rows:
            return
        table_name = self._table_name(connection_id)
        self.seadb_api.insert_rows(self.base_id, table_name, rows)

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
            GeneralTaskTable.status.name,
            GeneralTaskTable.size.name,
            GeneralTaskTable.priority.name,
            GeneralTaskTable.assignees.name,
            GeneralTaskTable.participants.name,
        ]
        desired_values = {column_name: set() for column_name in target_columns}
        for task in tasks:
            status = task.get('status')
            size = task.get('size')
            priority = task.get('priority')
            assignees = task.get('assignees')
            participants = task.get('participants')
            if status:
                desired_values[GeneralTaskTable.status.name].add(str(status))
            if size:
                desired_values[GeneralTaskTable.size.name].add(str(size))
            if priority:
                desired_values[GeneralTaskTable.priority.name].add(str(priority))
            if isinstance(assignees, list):
                for item in assignees:
                    if item:
                        desired_values[GeneralTaskTable.assignees.name].add(str(item))
            if isinstance(participants, list):
                for item in participants:
                    if item:
                        desired_values[GeneralTaskTable.participants.name].add(str(item))
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

    def get_total_count(self, connection_id):
        table_name = self._table_name(connection_id)
        sql = f"SELECT COUNT(*) as total FROM `{table_name}` WHERE deleted = false OR deleted IS NULL"
        results = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
        return int(results[0].get('total', 0)) if results else 0
