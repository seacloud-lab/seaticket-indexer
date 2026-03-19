import json
import logging

from seaqa_indexer.utils import need_update_connection_status
from seaqa_indexer.index.index_store.base_index import BaseIndex
from seaqa_indexer.utils.constants import SEASEARCH_BULK_OPERATE_LIMIT

logger = logging.getLogger(__name__)


class SeafileIndex(BaseIndex):
    mapping = {
        'properties': {
            'connection_id': {
                'type': 'keyword',
            },
            'repo_id': {
                'type': 'keyword',
            },
            'path': {
                'type': 'keyword'
            },
            'title': {
                'type': 'text',
                'analyzer': 'gse_standard_analyzer',
                'fields': {
                    'keyword': {
                        'type': 'keyword'
                    }
                }
            },
            'content': {
                'type': 'text',
                'analyzer': 'gse_standard_analyzer',
                'highlightable': True
            },
            "ai_summary": {
                "type": "keyword"
            },
            'modified_time': {
                'type': 'date',
            },
        }
    }

    def get_search_param(self, index_name, keyword, start, size, time_from, time_to, search_filename_only=False):
        query_map = {'bool': {'should': [], 'minimum_should_match': 1}}
        searches = self._make_query_searches(keyword, search_filename_only)
        query_map['bool']['should'] = searches

        query_map = self._add_time_range_filter(query_map, time_from, time_to)

        data = {
            'query': query_map,
            'from': start,
            'size': size,
            '_source': ['connection_id', 'repo_id', 'path', 'title', 'content', 'ai_summary', 'modified_time'],
            'sort': ['_score'],
            'highlight': {
                'pre_tags': ['<mark>'],
                'post_tags': ['</mark>'],
                'fields': {'content': {}},
            }
        }
        repo_query_info = {
            'index': index_name,
            'query': data
        }
        return repo_query_info

    def update(self, index_name, connection_id, repo_id, connection_status, seaqa_db, seafile_seadb_api, since):
        index_count = 0
        page_size = 1000
        offset = 0
        deleted_index_id_set = set()
        bulk_params = []

        while True:
            files = seafile_seadb_api.get_files_by_page(connection_id, since, page_size, offset)
            offset += page_size
            if not files:
                break

            for file in files:
                parent_dir = file.get('path')
                title = file.get('title')
                deleted = file.get('deleted')
                outdated = file.get('outdated')
                pk = file.get('_pk')
                key = str(pk)
                if deleted or outdated:
                    deleted_index_id_set.add(key)
                else:
                    doc = {
                        'connection_id': str(connection_id),
                        'repo_id': repo_id,
                        'path': parent_dir,
                        'title': title,
                        'modified_time': file.get('modified_time'),
                        'content': file.get('content'),
                        'ai_summary': file.get('ai_summary')
                    }
                    index_info = {'index': {'_index': index_name, '_id': key}}
                    bulk_params.append(index_info)
                    bulk_params.append(doc)

                    if len(bulk_params) >= SEASEARCH_BULK_OPERATE_LIMIT:
                        self.seasearch_api.bulk(index_name, bulk_params)
                        bulk_params = []

                    index_count += 1
                    if need_update_connection_status(index_count):
                        connection_status['last_indexed_count'] = index_count
                        seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
        if bulk_params:
            self.seasearch_api.bulk(index_name, bulk_params)

        if deleted_index_id_set:
            self.delete_indexes_by_ids(index_name, deleted_index_id_set)

        return index_count
