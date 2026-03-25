import logging

from seaqa_indexer.index.index_store.base_extra_source_vector_index import BaseExtraSourceVectorIndex
from seaqa_indexer.utils.constants import SEASEARCH_BULK_OPERATE_LIMIT


logger = logging.getLogger(__name__)


class TicketSummaryVectorIndex(BaseExtraSourceVectorIndex):
    source_type = 'ticket_summary'

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
                if pending_pks:
                    self.delete_indexes_by_pks(index_name, pending_pks)
                    pending_pks = set()
                self.seasearch_api.bulk(index_name, bulk_params)
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
                    "pk": str(pk),
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
