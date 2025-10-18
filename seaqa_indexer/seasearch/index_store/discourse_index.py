import logging
import json

from seaqa_indexer.utils import md5
from seaqa_indexer.config import LANG
from seaqa_indexer.utils import need_update_connection_status


logger = logging.getLogger(__name__)
SEASEARCH_BULK_OPETATE_LIMIT = 100


class DiscourseIndex(object):
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
            'body': {
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
            'bumped_at': {
                'type': 'date'
            },
            'created_at': {
                'type': 'date'
            },
        }
    }

    index_settings = {
        'analysis': {
            'analyzer': {
                'gse_standard_analyzer': {
                    'type': 'gse_standard',
                    "char_filter": [
                        "gse_filter"
                    ]
                },
            },
            "char_filter": {
                "gse_filter": {
                    "type": "mapping",
                    "mappings": [
                        "\n =>  "
                    ]
                }
            }
        }
    }

    def __init__(self, seasearch_api, shard_num=1):
        self.seasearch_api = seasearch_api
        self.shard_num = shard_num
        self.lang = LANG

    def create_index_if_missing(self, index_name):
        if self.lang != 'chinese':
            self.mapping['properties']['title']['analyzer'] = 'standard'
            self.mapping['properties']['body']['analyzer'] = 'standard'
            self.index_settings['analysis'].pop('char_filter', None)
            self.index_settings['analysis']['analyzer'].pop('gse_standard_analyzer', None)

        if not self.seasearch_api.check_index_mapping(index_name).get('is_exist'):
            data = {
                'shard_num': self.shard_num,
                'mappings': self.mapping,
                'settings': self.index_settings
            }
            self.seasearch_api.create_index(index_name, data)

    def check_index(self, index_name):
        return self.seasearch_api.check_index_mapping(index_name).get('is_exist')

    def _make_query_searches(self, keyword, search_title_only):
        match_query_kwargs = {'minimum_should_match': '-25%'}

        def _make_match_query(field, key_word, **kw):
            q = {'query': key_word}
            q.update(kw)
            return {'match': {field: q}}

        searches = []
        searches.append(_make_match_query('title', keyword, **match_query_kwargs))
        searches.append({
            'match': {
                'title.ngram': {
                    'query': keyword,
                    'minimum_should_match': '80%',
                }
            }
        })
        if not search_title_only:
            searches.append(_make_match_query('body', keyword, **match_query_kwargs))

        return searches

    def _ensure_filter_exists(self, query_map):
        if 'filter' not in query_map['bool']:
            query_map['bool']['filter'] = []
        return query_map

    def get_search_param(self, index_name, keyword, start=0, size=10, search_title_only=False):
        query_map = {'bool': {'should': [], 'minimum_should_match': 1}}
        searches = self._make_query_searches(keyword, search_title_only)
        query_map['bool']['should'] = searches

        data = {
            'query': query_map,
            'from': start,
            'size': size,
            '_source': ['connection_id', 'topic_id', 'title', 'body', 'slug', 'bumped_at', 'created_at'],
            'sort': [
                '_score'
            ],
            'highlight': {
                'pre_tags': ['<mark>'],
                'post_tags': ['</mark>'],
                'fields': {
                    'body': {}
                }
            }
        }

        repo_query_info = {
            'index': index_name,
            'query': data
        }

        return repo_query_info

    def search(self, index_name, keyword, start, size, search_filename_only=False):
        bulk_search_params = []
        search_param = self.get_search_param(index_name, keyword, start, size, search_filename_only)
        bulk_search_params.append(search_param)

        query_body = json.dumps({
            'index_queries': bulk_search_params
        })
        results = self.seasearch_api.unified_search(query_body)

        hits = results.get('hits', {}).get('hits', [])
        total = results.get('hits', {}).get('total', {}).get('value', 0)
        if not hits:
            return hits, total
        discourse_posts = []
        for hit in hits:
            source = hit['_source']

            post = {
                'connection_id': source['connection_id'],
                'topic_id': source['topic_id'],
                'title': source['title'],
                'body': source['body'],
                'category_id': source.get('category_id'),
                'slug': source['slug'],
                'bumped_at': source['bumped_at'],
                'created_at': source['created_at'],
            }
            discourse_posts.append(post)

        logger.debug(f"Discourse search: {keyword} in {index_name}, found {len(discourse_posts)} results")
        return discourse_posts

    def gen_index_id(self, topic_id):
        return md5(f"{topic_id}")

    def normal_search(self, index_name, dsl):
        doc_item = self.seasearch_api.normal_search(index_name, dsl)
        total = doc_item['hits']['total']['value']
        return doc_item['hits']['hits'], total

    def delete_indexes_by_ids(self, index_name, ids):
        delete_params = []
        for _id in ids:
            delete_params.append({'delete': {'_id': _id, '_index': index_name}})
            if len(delete_params) >= SEASEARCH_BULK_OPETATE_LIMIT:
                self.seasearch_api.bulk(index_name, delete_params)
                delete_params = []
        if delete_params:
            self.seasearch_api.bulk(index_name, delete_params)

    def delete_index_by_index_name(self, index_name):
        self.seasearch_api.delete_index_by_name(index_name)

    def update(self, index_name, connection_id, connection_status, seaqa_db, discourse_seadb_api, since):
        index_count = 0
        page_size = 1000
        offset = 0
        need_cleared_topic_id_set = set()
        need_cleared_topic_index_id_set = set()
        need_cleared_topics_pk_set = set()
        bulk_params = []
        while True:
            topics = discourse_seadb_api.get_topics_by_page(connection_id, since, page_size, offset)
            offset += page_size
            if not topics:
                break
            for topic in topics:
                topic_id = topic.get('topic_id')
                replies = []
                replies_offset = 0
                replies_page_size = 1000
                while True:
                    replies_data = discourse_seadb_api.get_topic_replies_by_page(connection_id, topic_id, replies_page_size, replies_offset)
                    if not replies_data:
                        break
                    replies.extend(replies_data)
                    replies_offset += replies_page_size

                key = self.gen_index_id(topic_id)
                if topic.get('deleted') == True:
                    need_cleared_topic_id_set.add(topic_id)
                    need_cleared_topic_index_id_set.add(key)
                    need_cleared_topics_pk_set.add(topic.get('_pk'))
                else:
                    body_content = ''
                    for reply in replies:
                        content = reply.get('content')
                        if content:
                            if body_content:
                                body_content += '\n\n' + content
                            else:
                                body_content = content

                    doc = {
                        'connection_id': connection_id,
                        'topic_id': topic['topic_id'],
                        'title': topic['title'],
                        'body': body_content,
                        'category_id': topic.get('category_id'),
                        'slug': topic.get('slug', ''),
                        'bumped_at': topic.get('bumped_at'),
                        'created_at': topic.get('created_at'),
                    }

                    index_info = {'index': {'_index': index_name, '_id': key}}
                    bulk_params.append(index_info)
                    bulk_params.append(doc)
                    if len(bulk_params) >= SEASEARCH_BULK_OPETATE_LIMIT:
                        self.seasearch_api.bulk(index_name, bulk_params)
                        bulk_params = []

                    index_count += 1

                    if need_update_connection_status(index_count):
                        connection_status['last_indexed_count'] = index_count
                        seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))

        if need_cleared_topic_index_id_set:
            self.delete_indexes_by_ids(index_name, need_cleared_topic_index_id_set)
            discourse_seadb_api.delete_topics_from_db(connection_id, list(need_cleared_topics_pk_set), list(need_cleared_topic_id_set))

        return index_count
