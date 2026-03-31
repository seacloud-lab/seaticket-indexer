import datetime
import json
import logging

import requests

from seaqa_indexer.ai_processor.utils import generate_summary
from seaqa_indexer.handlers.base_handler import ConnectionHandler
from seaqa_indexer.index.utils import gen_index_name_by_type
from seaqa_indexer.seadb_models.general_task_seadb_api import GeneralTaskSeaDBAPI
from seaqa_indexer.seadb_models.models import GeneralTaskTable
from seaqa_indexer.utils import need_update_connection_last_sync_count
from seaqa_indexer.utils.constants import ConnectionType, IndexStatus, SEASEARCH_BULK_OPERATE_LIMIT, \
GENERAL_TASK_CONNECTION_SPEC

logger = logging.getLogger(__name__)


class GeneralTaskHandler(ConnectionHandler):
    connection_type = ConnectionType.GENERAL_TASK.value
    BATCH_SIZE = 50

    mapping = {
        'properties': {
            'connection_id': {'type': 'keyword'},
            'title': {'type': 'text'},
            'content': {'type': 'text'},
        }
    }

    def gen_table_name(self, connection_id):
        return GeneralTaskTable.gen_table_name(connection_id)

    def get_search_param(self, seasearch_api, index_name, keyword, start, size, time_from, time_to):
        query_map = {
            'bool': {
                'should': [
                    {'match': {'title': {'query': keyword, 'minimum_should_match': '-35%', 'boost': 0.7}}},
                    {'match': {'content': {'query': keyword, 'minimum_should_match': '-35%', 'boost': 0.3}}},
                ],
                'minimum_should_match': 1
            }
        }
        return {
            'index': index_name,
            'query': {
                'query': query_map,
                'from': start,
                'size': size,
                '_source': [
                    'connection_id', 'title', 'content',
                ],
                'sort': ['_score'],
            }
        }

    def delete_index_by_connection_id(self, seasearch_api, connection_id):
        index_name = gen_index_name_by_type(connection_id, self.connection_type)
        seasearch_api.delete_index_by_name(index_name)
        self.delete_summary_vector_index_by_connection_id(seasearch_api, connection_id)

    def format_vector_search_result(self, record, connection, result):
        return {
            'title': record.get('title', ''),
            'content': record.get('content', ''),
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
                summary, vector = generate_summary(full_content, ConnectionType.GENERAL_TASK.value, project_uuid)
                updates.append({
                    'pk': pk,
                    'row': {
                        'ai_summary': summary,
                        'ai_summary_vector': vector,
                        'ai_processed_time': datetime.datetime.now(datetime.UTC).isoformat()
                    }

               })
                ai_processing_count += 1
                if len(updates) >= self.BATCH_SIZE:
                    general_task_seadb_api.update_rows(connection_id, updates)
                    updates = []
            if updates:
                general_task_seadb_api.update_rows(connection_id, updates)
        return ai_processing_count

    def update_index(self, project_connection, seasearch_api, seaqa_db, since):
        connection_id = project_connection.id
        project_uuid = project_connection.project_uuid
        connection_type = project_connection.type
        connection_status = json.loads(project_connection.status)
        index_name = gen_index_name_by_type(connection_id, connection_type)

        if not seasearch_api.check_index_mapping(index_name).get('is_exist'):
            seasearch_api.create_index(index_name, {'shard_num': 1, 'mappings': self.mapping})

        general_task_seadb_api = GeneralTaskSeaDBAPI(project_uuid)
        bulk_params = []
        deleted_index_ids = set()
        index_count = 0
        page_size = 1000
        offset = 0
        while True:
            rows = general_task_seadb_api.get_rows_by_page(connection_id, page_size, offset)
            if not rows:
                break

            for row in rows:
                pk = str(row.get('_pk'))
                if row.get('deleted') or row.get('outdated'):
                    deleted_index_ids.add(pk)
                    continue
                doc = {
                    'connection_id': str(connection_id),
                    'title': row.get('title', ''),
                    'content': row.get('content', ''),
                }
                bulk_params.append({'index': {'_index': index_name, '_id': pk}})
                bulk_params.append(doc)
                index_count += 1
                if len(bulk_params) >= SEASEARCH_BULK_OPERATE_LIMIT:
                    seasearch_api.bulk(index_name, bulk_params)
                    bulk_params = []

            if len(rows) < page_size:
                break
            offset += page_size

        if bulk_params:
            seasearch_api.bulk(index_name, bulk_params)

        if deleted_index_ids:
            delete_bulk = []
            for _id in deleted_index_ids:
                delete_bulk.append({'delete': {'_index': index_name, '_id': _id}})
                if len(delete_bulk) >= SEASEARCH_BULK_OPERATE_LIMIT:
                    seasearch_api.bulk(index_name, delete_bulk)
                    delete_bulk = []
            if delete_bulk:
                seasearch_api.bulk(index_name, delete_bulk)

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
        endpoint =f"{base_url.rstrip('/')}/tasks/"
        params = {}
        if old_last_sync_time is not None:
            params['since_at'] = str(old_last_sync_time)

        headers = {}
        if auth_token:
            headers['Authorization'] = f'Bearer {auth_token}'

        try:
            resp = requests.get(endpoint, params=params, headers=headers, timeout=60)
        except Exception as error:
            sync_logger.log(f'Fetch tasks failed: {error}')
            return None, None, None, None

        if resp.status_code >= 400:
            sync_logger.log(f'Fetch tasks failed: {resp.text}')
            return None, None, None, None
        data = resp.json()
        tasks = data.get('tasks', [])
        general_task_seadb_api = GeneralTaskSeaDBAPI(project_uuid)
        try:
            general_task_seadb_api.ensure_task_column_options(connection_id, tasks)
        except Exception as error:
            sync_logger.log(f'Ensure task column options failed: {error}')
        sync_count = 0
        status_last_update_time = datetime.datetime.now(datetime.UTC)
        sync_time = status_last_update_time.isoformat()
        status_last_update_count = 0

        spec_keys = GENERAL_TASK_CONNECTION_SPEC.keys()
        for task in tasks:
            source_row_id = str(task.get('id') or '').strip()
            if not source_row_id:
                continue

            existing_pk = general_task_seadb_api.get_pk_by_source_row_id(connection_id, source_row_id)
            row_data = {key: task.get(key) for key in spec_keys if key != 'id' and key != 'description'}
            others = task.get('others')
            if isinstance(others, (dict, list)):
                others = json.dumps(others, ensure_ascii=False)
            elif others is None:
                others = ''
            else:
                others = str(others)
            participants = task.get('participants')
            if not isinstance(participants, list):
                participants = []
            row_data.update({
                'source_row_id': source_row_id,
                'content': task.get('description', ''),
                'participants': participants,
                'others': others,
                'sync_time': sync_time,
                'deleted': False
            })
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

        total_count = general_task_seadb_api.get_total_count(connection_id)
        return sync_count, total_count, False, config

    def clean_connection_data(self, storage, seadb_api, project_uuid, connection_id, tables):
        if not tables:
            return
        table_name = self.gen_table_name(connection_id)
        for table in tables:
            if table.get('name') == table_name:
                seadb_api.delete_table(project_uuid, table.get('id'))
