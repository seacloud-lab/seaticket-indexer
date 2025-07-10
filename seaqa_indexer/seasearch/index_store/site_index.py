import json
import logging

from seaqa_indexer.utils import md5
from seaqa_indexer.config import LANG

logger = logging.getLogger(__name__)

SEASEARCH_BULK_OPETATE_LIMIT = 100

class SiteIndex(object):
    mapping = {
        'properties': {
            'site_id': {
                'type': 'keyword'
            },
            'url': {
                'type': 'keyword'
            },
            'etag': {
                'type': 'keyword'
            },
            'title': {
                'type': 'text',
                'fields': {
                    'ngram': {
                        'type': 'text',
                        'index': True,
                        'analyzer': 'site_title_ngram_analyzer',
                    },
                },
            },
            'content': {
                'type': 'text',
                'analyzer': 'gse_standard_analyzer',
                'highlightable': True
            },
            'last_modified': {
                'type': 'text'
            }
        }
    }

    index_settings = {
        'analysis': {
            'analyzer': {
                'site_title_ngram_analyzer': {
                    'type': 'custom',
                    'tokenizer': 'site_title_ngram_tokenizer',
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
            "char_filter": {
                "gse_filter": {
                    "type": "mapping",
                    "mappings": [
                        "\n =>  "
                    ]
                }
            },
            'tokenizer': {
                'site_title_ngram_tokenizer': {
                    'type': 'ngram',
                    'min_gram': 4,
                    'max_gram': 4,
                    'token_chars': [
                        'letter',
                        'digit'
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
            self.mapping['properties']['content']['analyzer'] = 'standard'
            self.index_settings['analysis'].pop('char_filter')
            self.index_settings['analysis']['analyzer'].pop('gse_standard_analyzer')
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
            searches.append(_make_match_query('content', keyword, **match_query_kwargs))

        return searches

    def _ensure_filter_exists(self, query_map):
        if 'filter' not in query_map['bool']:
            query_map['bool']['filter'] = []
        return query_map

    def search(self, project_uuid, keyword, start, size, search_title_only=False):
        bulk_search_params = []
        query_map = {'bool': {'should': [], 'minimum_should_match': 1}}
        searches = self._make_query_searches(keyword, search_title_only)
        query_map['bool']['should'] = searches

        data = {
            'query': query_map,
            'from': start,
            'size': size,
            '_source': ['site_id', 'url', 'title', 'content'],
            'sort': ['_score'],
            'highlight': {
                'pre_tags': ['<mark>'],
                'post_tags': ['</mark>'],
                'fields': {'content': {}},
            }
        }
        index_name = project_uuid
        repo_query_info = {
            'index': index_name,
            'query': data
        }
        bulk_search_params.append(repo_query_info)

        query_body = json.dumps({
            'index_queries': bulk_search_params
        })
        results = self.seasearch_api.unified_search(query_body)
        site_docs = []

        hits = results.get('hits', []).get('hits', [])
        total = results.get('hits', {}).get('total', {}).get('value', 0)

        if not hits:
            return site_docs

        for hit in hits:
            source = hit.get('_source')
            score = hit.get('_score')
            _id = hit.get('_id')
            r = {
                'title': source['title'],
                'url': source['url'],
                'site_id': source['site_id'],
                'content': source['content'],
                'score': score,
                '_id': _id,
            }
            # if highlight_content := hit.get('highlight', {}).get('content', [None])[0]:
            #     r.update(content=highlight_content)
            site_docs.append(r)

        logger.debug('search keyword: %s, in project: %s , \nsearch result: %s', keyword, project_uuid, site_docs)

        return site_docs

    def update_document(self, index_name, site_id, content, url, etag, title, last_modified):
        self.add_document(index_name, site_id, content, url, etag, title, last_modified)

    def add_document(self, index_name, site_id, content, url, etag, title, last_modified):
        bulk_add_params = []
        index_info = {'index': {'_index': index_name, '_id': md5(url)}}
        doc_info = {
            'site_id': site_id,
            'content': content,
            'url': url,
            'etag': etag,
            'title': title,
            'last_modified': last_modified,
        }

        bulk_add_params.append(index_info)
        bulk_add_params.append(doc_info)
        self.seasearch_api.bulk(index_name, bulk_add_params)

    def query_data_by_site(self, index_name, site_id, start, size):
        dsl = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"site_id": site_id}}
                    ]
                }
            },
            "from": start,
            "size": size,
            "_source": ["url", "last_modified", "etag"],
            "sort": ["@url"],
        }

        hits, total = self.normal_search(index_name, dsl)
        return hits, total

    def normal_search(self, index_name, dsl):
        doc_item = self.seasearch_api.normal_search(index_name, dsl)
        total = doc_item['hits']['total']['value']

        return doc_item['hits']['hits'], total

    def delete_indexes_by_urls(self, index_name, urls):
        delete_params = []
        for url in urls:
            delete_params.append({'delete': {'_id': md5(url), '_index': index_name}})
            if len(delete_params) >= SEASEARCH_BULK_OPETATE_LIMIT:
                self.seasearch_api.bulk(index_name, delete_params)
                delete_params = []
        if delete_params:
            self.seasearch_api.bulk(index_name, delete_params)

    def delete_index_by_index_name(self, index_name):
        self.seasearch_api.delete_index_by_name(index_name)
