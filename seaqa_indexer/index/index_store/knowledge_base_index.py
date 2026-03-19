import logging
from seaqa_indexer.index.index_store.base_index import BaseIndex
from seaqa_indexer.utils.constants import SEASEARCH_BULK_OPERATE_LIMIT


logger = logging.getLogger(__name__)


class KnowledgeBaseIndex(BaseIndex):
    mapping = {
        'properties': {
            'project_uuid': {
                'type': 'keyword'
            },
            'title': {
                'type': 'text',
                'analyzer': 'gse_standard_analyzer',
                'fields': {'keyword': {'type': 'keyword'}}
            },
            'content': {
                'type': 'text',
                'analyzer': 'gse_standard_analyzer',
                'highlightable': True
            },
            "ai_summary": {
                "type": "keyword"
            },
            'modified_time': {'type': 'date'}
        }
    }

    def get_search_param(self, index_name, keyword, start, size, time_from=None, time_to=None, search_title_only=False):
        query_map = {'bool': {'should': [], 'minimum_should_match': 1}}
        searches = self._make_query_searches(keyword, search_title_only)
        query_map['bool']['should'] = searches

        query_map = self._add_time_range_filter(query_map, time_from, time_to)

        data = {
            'query': query_map,
            'from': start,
            'size': size,
            '_source': ['title', 'project_uuid', 'content', 'ai_summary', 'modified_time'],
            'sort': ['_score'],
            'highlight': {
                'pre_tags': ['<mark>'],
                'post_tags': ['</mark>'],
                'fields': {'content': {}},
            }
        }
        return {'index': index_name, 'query': data}

    def update(self, index_name, project_uuid, kb_seadb_api, since):
        page_size = 1000
        bulk_params = []
        deleted_ids = set()
        deleted_pk_set = set()
        indexed_count = 0

        offset = 0
        while True:
            knowledge_bases = kb_seadb_api.get_knowledge_bases_by_page(since, page_size, offset)
            offset += page_size
            if not knowledge_bases:
                break
            for row in knowledge_bases:
                pk = row.get('_pk')
                title = row.get('title')
                content = row.get('content')
                ai_summary = row.get('ai_summary')
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
                    'ai_summary': ai_summary,
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
