import datetime
import json
import logging
import time

import requests

from seaqa_indexer.handlers.base_handler import ConnectionHandler
from seaqa_indexer.index.index_store.general_task_index import GeneralTaskIndex
from seaqa_indexer.index.utils import gen_index_name_by_type
from seaqa_indexer.seadb_models.general_task_seadb_api import GeneralTaskSeaDBAPI
from seaqa_indexer.seadb_models.models import GeneralTaskTable
from seaqa_indexer.utils import need_update_connection_last_sync_count
from seaqa_indexer.utils.constants import ConnectionType, IndexStatus, GENERAL_TASK_CONNECTION_SPEC

logger = logging.getLogger(__name__)


class GeneralTaskHandler(ConnectionHandler):
    connection_type = ConnectionType.GENERAL_TASK.value
    BATCH_SIZE = 50
    FETCH_PAGE_SIZE = 100

    def gen_table_name(self, connection_id):
        return GeneralTaskTable.gen_table_name(connection_id)

    def get_search_param(self, seasearch_api, index_name, keyword, start, size, time_from, time_to):
        general_task_index = GeneralTaskIndex(seasearch_api)
        return general_task_index.get_search_param(index_name, keyword, start, size, time_from, time_to)

    def delete_index_by_connection_id(self, seasearch_api, connection_id):
        index_name = gen_index_name_by_type(connection_id, self.connection_type)
        general_task_index = GeneralTaskIndex(seasearch_api)
        general_task_index.delete_index_by_index_name(index_name)
        self.delete_summary_vector_index_by_connection_id(seasearch_api, connection_id)

    def format_vector_search_result(self, record, connection, result):
        return {
            'title': record.get('title', ''),
            'modified_time': record.get('modified_time', ''),
            'status': record.get('status', ''),
            'priority': record.get('priority', ''),
        }

    def process_ai(self, seadb_api, storage, connection_id, project_uuid):
        page_size = 100
        updates = []
        ai_processing_count = 0
        general_task_seadb_api = GeneralTaskSeaDBAPI(project_uuid)
        while True:
            rows = general_task_seadb_api.get_need_ai_tasks_by_page(connection_id, page_size)
            if not rows:
                break

            for row in rows:
                pk = row.get('_pk')
                title = row.get('title', '')
                content = row.get('content', '')
                full_content = f"Title: {title}\n\nContent: {content}\n"
                summary = self.seaqa_ai_api.generate_summary(full_content, ConnectionType.GENERAL_TASK.value, project_uuid)
                vector = self.seaqa_ai_api.generate_embedding(summary, project_uuid)
                updates.append({
                    'pk': pk,
                    'row': {
                        'ai_summary': summary,
                        'ai_summary_vector': vector,
                        'ai_processed_time': datetime.datetime.now(datetime.UTC).isoformat(),
                    }
                })
                ai_processing_count += 1
                if len(updates) >= self.BATCH_SIZE:
                    general_task_seadb_api.update_rows(connection_id, updates)
                    updates = []
            if updates:
                general_task_seadb_api.update_rows(connection_id, updates)
                updates = []
        return ai_processing_count

    def update_index(self, project_connection, seasearch_api, seaqa_db, since):
        connection_id = project_connection.id
        connection_type = project_connection.type
        connection_status = json.loads(project_connection.status)
        project_uuid = project_connection.project_uuid
        general_task_seadb_api = GeneralTaskSeaDBAPI(project_uuid)
        general_task_index = GeneralTaskIndex(seasearch_api)

        connection_status['last_index_status'] = IndexStatus.INDEXING
        seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))

        index_name = gen_index_name_by_type(connection_id, connection_type)
        general_task_index.create_index_if_missing(index_name)
        index_count = general_task_index.update(index_name, connection_id, connection_status, seaqa_db, general_task_seadb_api, since)

        connection_status['last_indexed_count'] = index_count
        connection_status['last_index_status'] = IndexStatus.COMPLETED
        indexed_at = datetime.datetime.now(datetime.UTC)
        seaqa_db.update_connection(connection_id, indexed_at, json.dumps(connection_status))

    def sync(self, connection_id, project_uuid, config, old_last_sync_time, connection_status, seaqa_db, sync_logger):
        base_url = (config.get('base_url') or '').strip()
        auth_token = (config.get('auth_token') or '').strip()
        if not base_url:
            sync_logger.log('General Task config missing base_url')
            return None, None, None, None

        endpoint = base_url.rstrip('/')
        if not endpoint.endswith('/tasks'):
            endpoint = f'{endpoint}/tasks'
        endpoint = f'{endpoint}/'
        params = {}
        since = None
        if old_last_sync_time:
            since = old_last_sync_time.isoformat()
        sync_before = datetime.datetime.now(datetime.UTC).isoformat()
        params['since_at'] = since
        params['sync_before'] = sync_before
        params['limit'] = self.FETCH_PAGE_SIZE

        headers = {}
        if auth_token:
            headers['Authorization'] = f'Bearer {auth_token}'
        general_task_seadb_api = GeneralTaskSeaDBAPI(project_uuid)

        sync_count = 0
        status_last_update_time = datetime.datetime.now()
        status_last_update_count = 0

        spec_keys = GENERAL_TASK_CONNECTION_SPEC.keys()
        while True:
            try:
                resp = requests.get(endpoint, params=params, headers=headers, timeout=60)
                resp.raise_for_status()
                data = resp.json()
                if not isinstance(data, dict):
                    raise ValueError('General task adapter returned invalid JSON payload')
            except Exception as error:
                logger.error(error)
                sync_logger.log('Fetch tasks failed')
                return None, None, None, None

            tasks = data.get('tasks', [])
            user_mappings = data.get('user_mappings', [])
            if user_mappings:
                try:
                    general_task_seadb_api.upsert_user_mappings(connection_id, user_mappings)
                except Exception as error:
                    sync_logger.log(f'Upsert task user mappings failed: {error}')
            if tasks:
                try:
                    general_task_seadb_api.ensure_task_column_options(connection_id, tasks)
                except Exception as error:
                    sync_logger.log(f'Ensure task column options failed: {error}')

            for task in tasks:
                source_row_id = str(task.get('id') or '').strip()
                if not source_row_id:
                    continue

                existing_pk = general_task_seadb_api.get_pk_by_source_row_id(connection_id, source_row_id)
                task_data = {key: task.get(key) for key in spec_keys}
                row_data = general_task_seadb_api.build_row_data(task_data)
                if existing_pk:
                    general_task_seadb_api.update_rows(connection_id, [{
                        'pk': existing_pk,
                        'row': row_data,
                    }])
                else:
                    general_task_seadb_api.append_rows(connection_id, [row_data])
                sync_count += 1
                should_update_status, new_update_time, new_update_count = need_update_connection_last_sync_count(
                    sync_count,
                    status_last_update_time,
                    status_last_update_count
                )
                if should_update_status:
                    connection_status['last_sync_count'] = sync_count
                    seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
                    status_last_update_time = new_update_time
                    status_last_update_count = new_update_count

            page_info = data.get('page_info') or {}
            has_more = bool(page_info.get('has_more'))
            if not has_more:
                break
            next_cursor = page_info.get('next_cursor')
            if not next_cursor:
                sync_logger.log('Fetch tasks failed: missing next cursor in paged response')
                return None, None, None, None
            params['next_cursor'] = next_cursor

        total_count = general_task_seadb_api.get_total_count(connection_id)
        return sync_count, total_count, False, config, datetime.datetime.fromisoformat(sync_before)
