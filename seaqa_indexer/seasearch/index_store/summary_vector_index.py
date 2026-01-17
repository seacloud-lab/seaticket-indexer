import logging
import json

from seaqa_indexer.utils import need_update_connection_status
from seaqa_indexer.seasearch.index_store.vector_base_index import VectorBaseIndex
from seaqa_indexer.utils.constants import SEASEARCH_BULK_OPERATE_LIMIT

logger = logging.getLogger(__name__)


class SummaryVectorIndex(VectorBaseIndex):
    mapping = {
        'properties': {
            'project_uuid': {
                'type': 'keyword'
            },
            'connection_id': {
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

    def update(self, table_name, index_name, connection_id, project_uuid, connection_status, seaqa_db, seadb_api, since):
        index_count = 0
        page_size = 1000
        offset = 0
        deleted_index_pk_set = set()
        bulk_params = []
        pending_pks = set()

        def _flush_bulk_if_needed(force=False):
            nonlocal bulk_params, pending_pks
            if bulk_params and (force or len(bulk_params) >= SEASEARCH_BULK_OPERATE_LIMIT):
                # Delete old documents first, then insert new ones
                if pending_pks:
                    self.delete_indexes_by_pks(index_name, pending_pks)
                    pending_pks = set()
                self.seasearch_api.bulk(index_name, bulk_params)
                bulk_params = []

        while True:
            sql = f"""
                SELECT _pk, ai_summary, ai_summary_vector, deleted, outdated, modified_time 
                FROM `{table_name}` 
                WHERE ai_processed_time > '{since}'
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
                outdated = row.get('outdated')

                if deleted or outdated:
                    deleted_index_pk_set.add(pk)
                    continue

                # Skip if no AI summary or vector
                if not ai_summary or not ai_summary_vector:
                    logger.debug('Skip document without AI summary or vector, pk=%s', pk)
                    continue

                index_info = {"index": {"_index": index_name}}
                doc_info = {
                    "ai_summary": ai_summary,
                    "vec": ai_summary_vector,
                    "project_uuid": project_uuid,
                    "connection_id": connection_id,
                    "pk": pk,
                    "modified_time": modified_time
                }
                bulk_params.append(index_info)
                bulk_params.append(doc_info)
                pending_pks.add(pk)

                index_count += 1
                if need_update_connection_status(index_count):
                    connection_status['last_ai_indexed_count'] = index_count
                    seaqa_db.update_ai_index_status(connection_id, json.dumps(connection_status))

                _flush_bulk_if_needed()

            offset += page_size

        _flush_bulk_if_needed(force=True)

        if deleted_index_pk_set:
            self.delete_indexes_by_pks(index_name, deleted_index_pk_set)

        return index_count

    def semantic_search(self, index_names, query_vector, top_k=10, time_from=None, time_to=None):
        data = {
            "query_field": "vec",
            "k": top_k,
            "return_fields": ["ai_summary", "project_uuid", "connection_id", "pk", "modified_time"],
            "vector": query_vector,
            "_source": False
        }

        # Add time range filter if provided
        self._add_time_range_filter(data, time_from, time_to)

        search_results = self.seasearch_api.vector_search(
            index_names=index_names,
            data=data
        )

        hits = search_results.get('hits', {}).get('hits', [])

        topk_search_results = []
        for hit in hits:
            fields = hit.get('fields', {})
            topk_search_results.append({
                'ai_summary': fields.get('ai_summary', [''])[0],
                'project_uuid': fields.get('project_uuid')[0],
                'connection_id': fields.get('connection_id')[0],
                'pk': fields.get('pk')[0],
                'modified_time': fields.get('modified_time', [None])[0],
                'score': hit.get('_score', 0),
                'source_type': 'summary',
            })
        return topk_search_results
