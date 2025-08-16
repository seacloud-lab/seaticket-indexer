import logging
from datetime import datetime

from seaqa_indexer.utils import md5
from seaqa_indexer.config import LANG

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
                    'ngram': {
                        'type': 'text',
                        'index': True,
                        'analyzer': 'discourse_title_ngram_analyzer',
                    },
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
            'url': {
                'type': 'keyword'
            },
            'created_at': {
                'type': 'date',
                'format': 'epoch_millis'
            },
            'updated_at': {
                'type': 'date',
                'format': 'epoch_millis'
            }
        }
    }

    index_settings = {
        'analysis': {
            'analyzer': {
                'discourse_title_ngram_analyzer': {
                    'type': 'custom',
                    'tokenizer': 'discourse_title_ngram_tokenizer',
                    'filter': [
                        'lowercase',
                    ],
                },
                'gse_standard_analyzer': {
                    'type': 'gse_standard',
                    "char_filter": [
                        "gse_filter"
                    ]
                },
            },
            'tokenizer': {
                'discourse_title_ngram_tokenizer': {
                    'type': 'ngram',
                    'min_gram': 2,
                    'max_gram': 3,
                    'token_chars': [
                        'letter',
                        'digit'
                    ]
                }
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

    def get_search_param(self, index_name, keyword, start=0, size=10, search_title_only=False):
        searches = self._make_query_searches(keyword, search_title_only)
        
        dsl = {
            'query': {
                'bool': {
                    'should': searches
                }
            },
            'from': start,
            'size': size,
            'highlight': {
                'fields': {
                    'body': {}
                }
            },
            'sort': [
                {'_score': {'order': 'desc'}},
                {'updated_at': {'order': 'desc'}}
            ]
        }
        
        return {'index': index_name}, dsl

    def search(self, index_name, keyword, start, size, search_title_only=False):
        searches = self._make_query_searches(keyword, search_title_only)
        
        dsl = {
            'query': {
                'bool': {
                    'should': searches
                }
            },
            'from': start,
            'size': size,
            'highlight': {
                'fields': {
                    'body': {}
                }
            },
            'sort': [
                {'_score': {'order': 'desc'}},
                {'updated_at': {'order': 'desc'}}
            ]
        }
        
        hits, total = self._normal_search(index_name, dsl)
        
        discourse_posts = []
        for hit in hits:
            source = hit['_source']
            highlight = hit.get('highlight', {})
            
            post = {
                'connection_id': source['connection_id'],
                'topic_id': source['topic_id'],
                'title': source['title'],
                'body': source['body'],
                'category_id': source.get('category_id'),
                'url': source['url'],
                'created_at': source['created_at'],
                'updated_at': source['updated_at'],
                'highlight': highlight.get('body', [])
            }
            discourse_posts.append(post)
        
        logger.debug(f"Discourse search: {keyword} in {index_name}, found {len(discourse_posts)} results")
        return discourse_posts

    def add_or_update_topic_with_replies(self, index_name, connection_id, topic_data, replies_data, forum_url):
        _id = self.gen_topic_index_id(connection_id, topic_data['topic_id'])
        
        def convert_time_to_millisecond_timestamp(t):
            if not t:
                return None
            if isinstance(t, datetime):
                return int(t.timestamp() * 1000)
            return None

        topic_url = f"{forum_url}/t/{topic_data.get('slug', '')}/{topic_data['topic_id']}"
        
        all_replies_content = []
        for reply in replies_data:
            if reply.get('content'):
                all_replies_content.append(reply['content'])
        
        body_content = '\n\n'.join(all_replies_content) if all_replies_content else ''
        
        doc = {
            'connection_id': connection_id,
            'topic_id': topic_data['topic_id'],
            'title': topic_data['title'],
            'body': body_content,
            'category_id': topic_data.get('category_id'),
            'url': topic_url,
            'created_at': convert_time_to_millisecond_timestamp(topic_data.get('bumped_at')),
            'updated_at': convert_time_to_millisecond_timestamp(topic_data.get('bumped_at'))
        }

        bulk_ops = [
            {'index': {'_index': index_name, '_id': _id}},
            doc
        ]

        self.seasearch_api.bulk(index_name, bulk_ops)

    def gen_topic_index_id(self, connection_id, topic_id):
        return md5(f"{connection_id}_topic_{topic_id}")

    def delete_topic(self, index_name, connection_id, topic_id):
        topic_id_str = self.gen_topic_index_id(connection_id, topic_id)
        delete_ops = [{'delete': {'_index': index_name, '_id': topic_id_str}}]
        self.seasearch_api.bulk(index_name, delete_ops)

    def query_data_by_connection_id(self, index_name, connection_id, start, size):
        dsl = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"connection_id": connection_id}}
                    ]
                }
            },
            "from": start,
            "size": size,
            "_source": ["topic_id", "title", "post_type", "post_number", "url"],
            "sort": [{"updated_at": {"order": "desc"}}],
        }

        hits, total = self._normal_search(index_name, dsl)
        return hits, total

    def _normal_search(self, index_name, dsl):
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
