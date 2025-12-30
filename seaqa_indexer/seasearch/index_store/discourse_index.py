import logging
import json

from seaqa_indexer.utils import need_update_connection_status
from seaqa_indexer.utils.constants import INDEX_CONTENT_LIMIT
from seaqa_indexer.seasearch.index_store.base_index import BaseIndex
from seaqa_indexer.utils.constants import SEASEARCH_BULK_OPERATE_LIMIT


logger = logging.getLogger(__name__)


class DiscourseIndex(BaseIndex):
    mapping = {
        'properties': {
            'connection_id': {
                'type': 'keyword'
            },
            'topic_id': {
                'type': 'long'
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
            'category_id': {
                'type': 'long'
            },
            'slug': {
                'type': 'keyword'
            },
            'modified_time': {
                'type': 'date'
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
            '_source': ['connection_id', 'topic_id', 'title', 'content', 'slug', 'modified_time'],
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

    def update(self, index_name, connection_id, connection_status, seaqa_db, discourse_seadb_api, since):
        index_count = 0
        page_size = 1000
        offset = 0
        need_cleared_topic_index_id_set = set()
        bulk_params = []
        while True:
            topics = discourse_seadb_api.get_topics_by_page(connection_id, since, page_size, offset)
            offset += page_size
            if not topics:
                break
            for topic in topics:
                topic_id = topic.get('topic_id')
                pk = topic.get('_pk')
                key = str(pk)
                if topic.get('deleted') or topic.get('outdated'):
                    need_cleared_topic_index_id_set.add(key)
                else:
                    replies_offset = 0
                    replies_page_size = 1000
                    body_content = ''
                    while True:
                        replies_data = discourse_seadb_api.get_topic_replies_by_page(connection_id, topic_id,
                                                                                     replies_page_size, replies_offset)
                        if not replies_data or len(body_content) > INDEX_CONTENT_LIMIT:
                            break
                        for reply in replies_data:
                            body_content += reply.get('content') or ''
                        replies_offset += replies_page_size

                    doc = {
                        'connection_id': connection_id,
                        'topic_id': topic['topic_id'],
                        'title': topic['title'],
                        'content': body_content,
                        'category_id': topic.get('category_id'),
                        'slug': topic.get('slug', ''),
                        'modified_time': topic.get('modified_time'),
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
        if need_cleared_topic_index_id_set:
            self.delete_indexes_by_ids(index_name, need_cleared_topic_index_id_set)
        return index_count
