from seaqa_indexer.index.index_store.base_vector_index import BaseVectorIndex

class BaseExtraSourceVectorIndex(BaseVectorIndex):
    mapping = {
        'properties': {
            'pk': {
                'type': 'keyword'
            },
            'ai_summary': {
                'type': 'text'
            },
            'vec': {
                'type': 'vector',
                'dims': 1536,
                'm': 64,
                'nbits': 8,
                'vec_index_type': 'flat'
            },
            'modified_time': {
                'type': 'date'
            }
        }
    }

    def semantic_search(self, index_name, query_vector, top_k=10, time_from=None, time_to=None):
        data = {
            "query_field": "vec",
            "k": top_k,
            "return_fields": ["ai_summary", "pk", "modified_time"],
            "vector": query_vector,
            "_source": False
        }

        self._add_time_range_filter(data, time_from, time_to)

        search_results = self.seasearch_api.vector_search(
            index_names=[index_name],
            data=data
        )

        hits = search_results.get('hits', {}).get('hits', [])

        topk_search_results = []
        for hit in hits:
            fields = hit.get('fields', {})
            topk_search_results.append({
                'ai_summary': fields.get('ai_summary', [''])[0],
                'pk': fields.get('pk')[0],
                'modified_time': fields.get('modified_time', [None])[0],
                'score': hit.get('_score', 0),
                'source_type': self.source_type,
            })
        return topk_search_results
