import datetime
import json

from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.config import GENERAL_TASK_COLUMN_MAPPINGS
from seaqa_indexer.utils.constants import ConnectionType
from seaqa_indexer.seadb_models.models import SchemaTables


class GeneralTaskSeaDBAPI:
    def __init__(self, base_id):
        self.base_id = base_id
        self.seadb_api = SeaDBAPI()

    def _table_name(self, connection_id):
        return SchemaTables.GENERAL_TASK.table_name(connection_id=connection_id)

    def get_need_ai_tasks_by_page(self, connection_id, page_size):
        table_name = self._table_name(connection_id)
        sql = f"""
            SELECT _pk, title, status, size, priority, assignees, participants, others, content, modified_time, due_date, created_time
            FROM `{table_name}`
            WHERE (ai_processed_time IS NULL OR ai_processed_time < record_modified_time)
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
        if not source_task_ids:
            return {}
        table_name = self._table_name(connection_id)
        escaped_ids = "', '".join(source_task_id.replace("'", "''") for source_task_id in source_task_ids)
        sql = (
            f"SELECT _pk, source_task_id, url, title, status, size, priority, assignees, participants, "
            f"version, others, content, due_date, modified_time, created_time, sync_time, deleted, "
            f"outdated, record_modified_time, linked_ticket "
            f"FROM `{table_name}` "
            f"WHERE source_task_id IN ('{escaped_ids}')"
        )
        results = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
        return {row.get('source_task_id'): row for row in results }

    def get_all_existing_user_rows_by_ids(self, user_ids):
        """Query existing user rows by user_id only (any connection).

        Returns a dict: {user_id: {'_pk': ..., 'connection_id': [...]}}
        """
        if not user_ids:
            return {}
        table_name = SchemaTables.CONNECTION_USER.table_name()
        escaped_ids = "', '".join(uid.replace("'", "''") for uid in user_ids)
        sql = f"SELECT user_id, _pk, connection_ids FROM `{table_name}` WHERE user_id IN ('{escaped_ids}') AND connection_type='general_task'"
        results = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
        return {row.get('user_id'): {'_pk': row.get('_pk'), 'connection_ids': row.get('connection_ids') or []} for row in results}

    def append_rows(self, connection_id, rows):
        if not rows:
            return []
        table_name = self._table_name(connection_id)
        resp = self.seadb_api.insert_rows(self.base_id, table_name, rows) or {}
        return resp.get('pks') or []

    def get_row_by_source_task_id(self, connection_id, source_task_id):
        rows = self.get_rows_by_source_task_ids(connection_id, [source_task_id])
        return rows.get(source_task_id)

    def upsert_user_mappings(self, connection_id, user_mappings):
        """Upsert user mappings with merge logic.

        Same user_id across different connections is stored once with
        connection_id list aggregated. Uses int64 list for connection_id.
        """
        if not user_mappings:
            return
        now = datetime.datetime.now(datetime.UTC).isoformat()

        # Collect user_ids from mappings
        user_ids = [str(user_mapping.get('email') or '').strip() for user_mapping in user_mappings]
        user_ids = [uid for uid in user_ids if uid]

        # Query ALL existing rows by user_id (not scoped to connection_id)
        existing_user_row_map = self.get_all_existing_user_rows_by_ids(user_ids) if user_ids else {}

        insert_users = []
        update_users = []
        for user_mapping in user_mappings:
            user_id = str(user_mapping.get('email') or '').strip()
            name = str(user_mapping.get('name') or '').strip()
            if not user_id or not name:
                continue
            existing_user = existing_user_row_map.get(user_id)
            if existing_user:
                existing_conn_ids = existing_user['connection_ids']
                if connection_id in existing_conn_ids:
                    # Already known in this connection — just refresh name/time
                    update_users.append({
                        'pk': existing_user['_pk'],
                        'row': {
                            SchemaTables.CONNECTION_USER.column.name.name: name,
                            SchemaTables.CONNECTION_USER.column.record_modified_time.name: now,
                        }
                    })
                else:
                    # User exists in other connections — append this connection_id
                    existing_conn_ids.append(connection_id)
                    update_users.append({
                        'pk': existing_user['_pk'],
                        'row': {
                            SchemaTables.CONNECTION_USER.column.name.name: name,
                            SchemaTables.CONNECTION_USER.column.connection_ids.name: existing_conn_ids,
                            SchemaTables.CONNECTION_USER.column.record_modified_time.name: now,
                        }
                    })
            else:
                # Brand new user
                insert_users.append({
                    SchemaTables.CONNECTION_USER.column.user_id.name: user_id,
                    SchemaTables.CONNECTION_USER.column.name.name: name,
                    SchemaTables.CONNECTION_USER.column.connection_ids.name: [connection_id],
                    SchemaTables.CONNECTION_USER.column.connection_type.name: ConnectionType.GENERAL_TASK.value,
                    SchemaTables.CONNECTION_USER.column.record_modified_time.name: now,
                })

        table_name = SchemaTables.CONNECTION_USER.table_name()
        if insert_users:
            self.seadb_api.insert_rows(self.base_id, table_name, insert_users)
        if update_users:
            self.seadb_api.update_rows(self.base_id, table_name, update_users)

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
            SchemaTables.GENERAL_TASK.column.status.name,
            SchemaTables.GENERAL_TASK.column.size.name,
            SchemaTables.GENERAL_TASK.column.priority.name,
        ]
        desired_values = {column_name: set() for column_name in target_columns}
        for task in tasks:
            status = task.get('status')
            size = task.get('size')
            priority = task.get('priority')
            if status:
                desired_values[SchemaTables.GENERAL_TASK.column.status.name].add(str(status))
            if size:
                desired_values[SchemaTables.GENERAL_TASK.column.size.name].add(str(size))
            if priority:
                desired_values[SchemaTables.GENERAL_TASK.column.priority.name].add(str(priority))
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
            SchemaTables.GENERAL_TASK.column.source_task_id.name: str(task.get('task_id') or '').strip(),
            SchemaTables.GENERAL_TASK.column.url.name: str(task.get('url') or '').strip(),
            SchemaTables.GENERAL_TASK.column.title.name: task.get('title', ''),
            SchemaTables.GENERAL_TASK.column.status.name: task.get('status'),
            SchemaTables.GENERAL_TASK.column.size.name: task.get('size'),
            SchemaTables.GENERAL_TASK.column.priority.name: task.get('priority'),
            SchemaTables.GENERAL_TASK.column.assignees.name: task.get('assignees', []),
            SchemaTables.GENERAL_TASK.column.participants.name: task.get('participants', []),
            SchemaTables.GENERAL_TASK.column.version.name: task.get('version', ''),
            SchemaTables.GENERAL_TASK.column.others.name: others,
            SchemaTables.GENERAL_TASK.column.content.name: content,
            SchemaTables.GENERAL_TASK.column.due_date.name: task.get('due_date'),
            SchemaTables.GENERAL_TASK.column.modified_time.name: task.get('modified_time') or now,
            SchemaTables.GENERAL_TASK.column.created_time.name: task.get('created_time') or now,
            SchemaTables.GENERAL_TASK.column.sync_time.name: datetime.datetime.now(datetime.UTC).isoformat(),
            SchemaTables.GENERAL_TASK.column.record_modified_time.name: now,
            SchemaTables.GENERAL_TASK.column.deleted.name: bool(task.get('deleted', False)),
        }

    def get_total_count(self, connection_id):
        table_name = self._table_name(connection_id)
        sql = f"SELECT COUNT(*) as total FROM `{table_name}` WHERE deleted = false OR deleted IS NULL"
        results = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
        return int(results[0].get('total', 0)) if results else 0
