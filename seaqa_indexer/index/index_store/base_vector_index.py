import logging

from abc import abstractmethod
from seaqa_indexer.index.index_store.base_index import BaseIndex
from seaqa_indexer.utils import time_to_utc_time

logger = logging.getLogger(__name__)


class BaseVectorIndex(BaseIndex):

    def create_index_if_missing(self, index_name):
        if not self.seasearch_api.check_index_mapping(index_name).get('is_exist'):
            self.seasearch_api.create_index(
                index_name,
                {"shard_num": self.shard_num, "mappings": self.mapping}
            )

    def delete_indexes_by_pks(self, index_name, pks):
        if not pks:
            return []

        pks_str_list = [str(pk) for pk in pks]
        page_size = 200
        while True:
            query = {
                "query": {
                    "terms": {
                        "pk": pks_str_list
                    }
                },
                "_source": False,
                "size": page_size,
                "from": 0
            }
            result = self.seasearch_api.normal_search(index_name, query)
            hits = result.get('hits', {}).get('hits', [])
            if not hits:
                break

            delete_params = []
            for hit in hits:
                delete_params.append({'delete': {'_id': hit.get('_id'), '_index': index_name}})

            if delete_params:
                self.seasearch_api.bulk(index_name, delete_params)
            if len(hits) < page_size:
                break

    def _ensure_filter_exists(self, data):
        if 'filter_query' not in data:
            data['filter_query'] = {'bool': {'filter': []}}
        return data

    def _add_time_range_filter(self, data, time_from, time_to):
        search_content = {}
        if time_from:
            search_content['gte'] = time_to_utc_time(time_from).isoformat()
        if time_to:
            search_content['lte'] = time_to_utc_time(time_to).isoformat()
        if not search_content:
            return data
        data = self._ensure_filter_exists(data)
        data['filter_query']['bool']['filter'].append({'range': {'modified_time': search_content}})
        return data
    
    @abstractmethod
    def semantic_search(self, *args, **kwargs):
        pass
