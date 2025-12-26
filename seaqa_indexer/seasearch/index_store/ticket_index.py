import json
import logging
import datetime

from seaqa_indexer.config import LANG

logger = logging.getLogger(__name__)


SEASEARCH_BULK_OPERATE_LIMIT = 100

class TicketIndex(object):
    mapping = {
        'properties': {
            'project_uuid': {
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
            'modified_time': {
                'type': 'date'
            }
        }
    }

    index_settings = {
        'analysis': {
            'analyzer': {
                'gse_standard_analyzer': { 
                    'type': 'gse_standard',
                    'char_filter': ['gse_filter']
                }
            },
            'char_filter': {
                'gse_filter': {
                    'type': 'mapping',
                    'mappings': ['\n =>  ']
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
            self.mapping['properties']['title']['analyzer'] = 'standard'
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
        match_query_kwargs = {'minimum_should_match': '-35%'}
        title_boost_kwargs = {'boost': 0.7}
        content_boost_kwargs = {'boost': 0.3}
        def _make_match_query(field, key_word, **kw):
            q = {'query': key_word}
            q.update(kw)
            return {'match': {field: q}}
        searches = []
        searches.append(_make_match_query('title', keyword, **match_query_kwargs, **title_boost_kwargs))
        if not search_title_only:
            searches.append(_make_match_query('content', keyword, **match_query_kwargs, **content_boost_kwargs))
        return searches

    def _ensure_filter_exists(self, query_map):
        if 'filter' not in query_map['bool']:
            query_map['bool']['filter'] = []
        return query_map

    def get_search_param(self, index_name, keyword, start, size, time_from=None, time_to=None, search_title_only=False):
        query_map = {'bool': {'should': [], 'minimum_should_match': 1}}
        searches = self._make_query_searches(keyword, search_title_only)
        query_map['bool']['should'] = searches

        if time_from or time_to:
            range_filter = {'range': {'modified_time': {}}}
            if time_from:
                if isinstance(time_from, datetime.datetime):
                    time_from = time_from.isoformat()
                range_filter['range']['modified_time']['gte'] = time_from
            if time_to:
                if isinstance(time_to, datetime.datetime):
                    time_to = time_to.isoformat()
                range_filter['range']['modified_time']['lte'] = time_to
            self._ensure_filter_exists(query_map)
            query_map['bool']['filter'].append(range_filter)

        data = {
            'query': query_map,
            'from': start,
            'size': size,
            '_source': ['title', 'content', 'modified_time'],
            'sort': ['_score'],
            'highlight': {
                'pre_tags': ['<mark>'],
                'post_tags': ['</mark>'],
                'fields': {'content': {}, 'title': {}},
            }
        }
        return {'index': index_name, 'query': data}

    def search(self, index_name, keyword, start=0, size=10, time_from=None, time_to=None, search_title_only=False):
        bulk_search_params = [self.get_search_param(index_name, keyword, start, size, time_from, time_to, search_title_only)]
        query_body = json.dumps({'index_queries': bulk_search_params})
        results = self.seasearch_api.unified_search(query_body)
        docs = []
        hits = results.get('hits', {}).get('hits', [])
        if not hits:
            return docs
        for hit in hits:
            source = hit.get('_source')
            score = hit.get('_score')
            _id = hit.get('_id')
            r = {
                'title': source['title'],
                'content': source['content'],
                'modified_time': source.get('modified_time'),
                'score': score,
                '_id': _id,
            }
            docs.append(r)
        return docs

    def normal_search(self, index_name, dsl):
        doc_item = self.seasearch_api.normal_search(index_name, dsl)
        total = doc_item.get('hits', {}).get('total', {}).get('value', 0)
        return doc_item.get('hits', {}).get('hits', []), total

    def delete_indexes_by_ids(self, index_name, ids):
        delete_params = []
        for _id in ids:
            delete_params.append({'delete': {'_id': _id, '_index': index_name}})
            if len(delete_params) >= SEASEARCH_BULK_OPERATE_LIMIT:
                self.seasearch_api.bulk(index_name, delete_params)
                delete_params = []
        if delete_params:
            self.seasearch_api.bulk(index_name, delete_params)

    def delete_index_by_index_name(self, index_name):
        self.seasearch_api.delete_index_by_name(index_name)

    def update(self, index_name, ticket_seadb_api, since, project_uuid):
        page_size = 1000
        bulk_params = []
        deleted_ids = set()
        deleted_pk_set = set()
        indexed_count = 0

        offset = 0
        while True:
            tickets = ticket_seadb_api.get_tickets_by_page(since, page_size, offset)
            offset += page_size
            if not tickets:
                break
            for row in tickets:
                pk = row.get('_pk')
                title = row.get('title')
                content = row.get('content')
                deleted = row.get('deleted')
                doc_id = str(pk)
                if deleted:
                    deleted_ids.add(doc_id)
                    deleted_pk_set.add(pk)
                    continue
                index_info = {'index': {'_index': index_name, '_id': doc_id}}
                doc_info = {
                    'project_uuid': project_uuid,
                    'title': title,
                    'content': content,
                    'modified_time': row.get('modified_time')
                }
                bulk_params.append(index_info)
                bulk_params.append(doc_info)
                indexed_count += 1
                if len(bulk_params) >= SEASEARCH_BULK_OPERATE_LIMIT:
                    self.seasearch_api.bulk(index_name, bulk_params)
                    bulk_params = []

            offset += page_size
        if bulk_params:
            self.seasearch_api.bulk(index_name, bulk_params)
        if deleted_ids:
            self.delete_indexes_by_ids(index_name, deleted_ids)
        
        return indexed_count

