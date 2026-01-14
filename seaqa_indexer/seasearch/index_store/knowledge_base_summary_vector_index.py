import logging

from seaqa_indexer.seasearch.index_store.vector_base_index import VectorBaseIndex
from seaqa_indexer.utils.constants import SEASEARCH_BULK_OPERATE_LIMIT

logger = logging.getLogger(__name__)


class KnowledgeBaseSummaryVectorIndex(VectorBaseIndex):
    mapping = {
        'properties': {
            'project_uuid': {
                'type': 'keyword'
            },
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

    def update(self, table_name, index_name, project_uuid, seadb_api, since):
        index_count = 0
        page_size = 1000
        offset = 0
        deleted_index_pk_set = set()
        bulk_params = []
        pending_pks = set()

        def _flush_bulk_if_needed(force=False):
            nonlocal bulk_params, pending_pks
            if bulk_params and (force or len(bulk_params) >= SEASEARCH_BULK_OPERATE_LIMIT):
                self.delete_indexes_by_pks(index_name, pending_pks)
                self.seasearch_api.bulk(index_name, bulk_params)
                pending_pks = set()
                bulk_params = []

        while True:
            sql = f"""
                SELECT _pk, ai_summary, ai_summary_vector, deleted, modified_time, ai_processed_time
                FROM `{table_name}`
                WHERE ai_processed_time IS NOT NULL
                AND ai_processed_time > '{since}'
                ORDER BY ai_processed_time
                LIMIT {page_size} OFFSET {offset}
            """
            result = seadb_api.query_rows(project_uuid, sql)
            rows = result.get('results', [])
            if not rows:
                break

            for row in rows:
                pk = row.get('_pk')
                ai_summary = row.get('ai_summary')
                ai_summary_vector = row.get('ai_summary_vector')
                deleted = row.get('deleted')
                modified_time = row.get('modified_time')

                if deleted:
                    deleted_index_pk_set.add(pk)
                    continue

                if not ai_summary or not ai_summary_vector:
                    logger.debug('Skip document without AI summary or vector, pk=%s', pk)
                    continue

                index_info = {"index": {"_index": index_name}}
                doc_info = {
                    "ai_summary": ai_summary,
                    "vec": ai_summary_vector,
                    "project_uuid": project_uuid,
                    "pk": pk,
                    "modified_time": modified_time
                }
                bulk_params.append(index_info)
                bulk_params.append(doc_info)
                pending_pks.add(pk)

                index_count += 1

                _flush_bulk_if_needed()

            offset += page_size

        _flush_bulk_if_needed(force=True)

        if deleted_index_pk_set:
            self.delete_indexes_by_pks(index_name, deleted_index_pk_set)

        return index_count

    def semantic_search(self, index_name, query_vector, top_k=10, time_from=None, time_to=None):
        data = {
            "query_field": "vec",
            "k": top_k,
            "return_fields": ["ai_summary", "project_uuid", "pk", "modified_time"],
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
                'project_uuid': fields.get('project_uuid')[0],
                'pk': fields.get('pk')[0],
                'modified_time': fields.get('modified_time', [None])[0],
                'score': hit.get('_score', 0),
                'source_type': 'knowledge_base_summary',
            })
        return topk_search_results
