import logging

from seaqa_indexer.utils import time_to_utc_time
from seaqa_indexer.config import LANG
from abc import ABC, abstractmethod
from seaqa_indexer.utils.constants import SEASEARCH_BULK_OPERATE_LIMIT

logger = logging.getLogger(__name__)


class BaseIndex(ABC):
    mapping = {}

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

    def _make_query_searches(self, keyword, search_filename_only):
        match_query_kwargs = {'minimum_should_match': '-35%'}
        title_boost_kwargs = {'boost': 0.7}
        content_boost_kwargs = {'boost': 0.3}

        def _make_match_query(field, key_word, **kw):
            q = {'query': key_word}
            q.update(kw)
            return {'match': {field: q}}

        searches = []
        searches.append(_make_match_query('title', keyword, **match_query_kwargs, **title_boost_kwargs))
        if not search_filename_only:
            searches.append(_make_match_query('content', keyword, **match_query_kwargs, **content_boost_kwargs))

        return searches

    def _ensure_filter_exists(self, query_map):
        if 'filter' not in query_map['bool']:
            query_map['bool']['filter'] = []
        return query_map

    def _add_time_range_filter(self, query_map, time_from, time_to):
        search_content = {}
        if time_from:
            search_content['gte'] = time_to_utc_time(time_from).isoformat()
        if time_to:
            search_content['lte'] = time_to_utc_time(time_to).isoformat()
        if not search_content:
            return query_map
        query_map = self._ensure_filter_exists(query_map)
        query_map['bool']['filter'].append({'range': {'modified_time': search_content}})
        return query_map

    def normal_search(self, index_name, dsl):
        doc_item = self.seasearch_api.normal_search(index_name, dsl)
        total = doc_item['hits']['total']['value']

        return doc_item['hits']['hits'], total

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

    @abstractmethod
    def update(self, *args, **kwargs):
        pass
