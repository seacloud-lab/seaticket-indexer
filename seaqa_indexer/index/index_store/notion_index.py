import json
import logging

from seaqa_indexer.utils import need_update_connection_status
from seaqa_indexer.utils.constants import INDEX_CONTENT_LIMIT, SEASEARCH_BULK_OPERATE_LIMIT
from seaqa_indexer.index.index_store.base_index import BaseIndex
from seaqa_indexer.utils import uuid_str_to_32_chars

logger = logging.getLogger(__name__)


class NotionIndex(BaseIndex):
    mapping = {
        'properties': {
            'connection_id': {
                'type': 'keyword',
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
            'page_id': {
                'type': 'keyword',
            },
            'modified_time': {
                'type': 'date',
            },
        }
    }

    def get_search_param(self, index_name, keyword, start, size, time_from, time_to, search_title_only=False):
        query_map = {'bool': {'should': [], 'minimum_should_match': 1}}
        searches = self._make_query_searches(keyword, search_title_only)
        query_map['bool']['should'] = searches

        query_map = self._add_time_range_filter(query_map, time_from, time_to)

        data = {
            'query': query_map,
            'from': start,
            'size': size,
            '_source': ['connection_id', 'page_id', 'title', 'content', 'modified_time'],
            'sort': [
                '_score'
            ],
            'highlight': {
                'pre_tags': ['<mark>'],
                'post_tags': ['</mark>'],
                'fields': {
                    'content': {}
                }
            }
        }

        repo_query_info = {
            'index': index_name,
            'query': data
        }

        return repo_query_info

    def update(self, index_name, connection_id, connection_status, seaqa_db, notion_seadb_api, since):
        index_count = 0
        page_size = 1000
        offset = 0
        need_cleared_pages_index_set = set()
        bulk_params = []

        while True:
            pages = notion_seadb_api.get_notion_pages_by_page(connection_id, since, page_size, offset)
            if not pages:
                break

            for notion_page in pages:
                pk = notion_page.get('_pk')
                key = str(pk)
                if notion_page.get('deleted') or notion_page.get('outdated'):
                    need_cleared_pages_index_set.add(key)
                else:
                    content = notion_page.get('content') or ''
                    if len(content) > INDEX_CONTENT_LIMIT:
                        content = content[:INDEX_CONTENT_LIMIT]

                    page_id = uuid_str_to_32_chars(notion_page.get('page_id'))

                    doc = {
                        'connection_id': str(connection_id),
                        'page_id': page_id,
                        'title': notion_page.get('title'),
                        'content': content,
                        'modified_time': notion_page.get('modified_time'),
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
            offset += page_size

        if bulk_params:
            self.seasearch_api.bulk(index_name, bulk_params)

        if need_cleared_pages_index_set:
            self.delete_indexes_by_ids(index_name, need_cleared_pages_index_set)

        return index_count
