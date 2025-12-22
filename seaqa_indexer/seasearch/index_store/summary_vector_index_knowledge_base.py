from seaqa_indexer.utils.seadb_api import SeaDBAPI
import datetime
import pytz

import logging

logger = logging.getLogger(__name__)

SEASEARCH_BULK_OPERATE_LIMIT = 100

class SummaryVectorIndexKnowledgeBase(object):
    mapping = {
        'properties': {
            'project_id': {
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

    def __init__(self, seasearch_api, seadb_api=None, shard_num=1):
        self.seasearch_api = seasearch_api
        self.seadb_api = seadb_api or SeaDBAPI()
        self.shard_num = shard_num

    def create_index_if_missing(self, index_name):
        if not self.seasearch_api.check_index_mapping(index_name).get('is_exist'):
            self.seasearch_api.create_index(
                index_name,
                {"shard_num": self.shard_num, "mappings": self.mapping}
            )  
    
    def check_index(self, index_name):
        return self.seasearch_api.check_index_mapping(index_name).get('is_exist')  

    def _query_index_ids_by_pks(self, index_name, pks):
        if not pks:
            return []

        pks_str_list = [str(pk) for pk in pks]
        doc_ids = []
        try:
            page_size = max(len(pks) * 10, 200)
            offset = 0
            while True:
                query = {
                    "query": {
                        "terms": {
                            "pk": pks_str_list
                        }
                    },
                    "_source": False,
                    "size": page_size,
                    "from": offset
                }
                result = self.seasearch_api.normal_search(index_name, query)
                hits = result.get('hits', {}).get('hits', [])
                if not hits:
                    break

                for hit in hits:
                    if hit.get('_id'):
                        doc_ids.append(hit.get('_id'))
                if len(hits) < page_size:
                    break
                offset += page_size
            logger.debug('Found %d index documents for %d pks', len(doc_ids), len(pks))
            return doc_ids
        except Exception as e:
            logger.warning('Failed to query index IDs: %s', str(e))
            return []

    def delete_indexes_by_pks(self, index_name, pks):
        if not pks:
            return

        doc_ids = self._query_index_ids_by_pks(index_name, pks)
        if not doc_ids:
            return

        delete_params = []
        for _id in doc_ids:
            delete_params.append({'delete': {'_id': _id, '_index': index_name}})
    
        if len(delete_params) >= SEASEARCH_BULK_OPERATE_LIMIT:
            self.seasearch_api.bulk(index_name, delete_params)
            delete_params = []
        if delete_params:
            self.seasearch_api.bulk(index_name, delete_params)
        logger.info('Deleted %d index documents', len(doc_ids))

    def update_kb(self, table_name, index_name, project_uuid, last_ai_indexed_at=None):
        index_count = 0
        page_size = 1000
        offset = 0
        deleted_index_id_set = set()
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

        if not last_ai_indexed_at:
            last_ai_indexed_at = datetime.datetime(1970, 1, 1)

        last_ai_indexed_at = last_ai_indexed_at.replace(tzinfo=pytz.UTC).isoformat()

        while True:
            sql = f"""
                SELECT _pk, ai_summary, ai_summary_vector, deleted, modified_time, ai_processed_time
                FROM `{table_name}`
                WHERE ai_processed_time IS NOT NULL
                AND ai_processed_time > '{last_ai_indexed_at}'
                ORDER BY ai_processed_time
                LIMIT {page_size} OFFSET {offset}
            """
            result = self.seadb_api.query_rows(project_uuid, sql)
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
                    deleted_index_id_set.add(pk)
                    continue

                if not ai_summary:
                    logger.debug('Skip KB document without AI summary, pk=%s', pk)
                    continue

                index_info = {"index": {"_index": index_name}}
                doc_info = {
                    "ai_summary": ai_summary,
                    "vec": ai_summary_vector,
                    "project_id": project_uuid,
                    "connection_id": None,
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

        if deleted_index_id_set:
            self.delete_indexes_by_pks(index_name, deleted_index_id_set)

        return index_count