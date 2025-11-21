import logging
import json
import datetime
import pytz

from seaqa_indexer.utils import need_update_connection_status
from seaqa_indexer.seadb_models.models import DiscourseTopicsTable
from seaqa_indexer.utils.seadb_api import SeaDBAPI

logger = logging.getLogger(__name__)

SEASEARCH_BULK_OPETATE_LIMIT = 100


class DiscourseAIVectorIndex(object):
    
    mapping = {
        'properties': {
            'project_id': {
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
        """Query index document IDs by pk field"""
        if not pks:
            return []
        
        pks_str_list = [str(pk) for pk in pks]
        try:
            query = {
                "query": {
                    "terms": {
                        "pk": pks_str_list
                    }
                },
                "_source": False,
                "size": len(pks)
            }
            result = self.seasearch_api.normal_search(index_name, query)
            hits = result.get('hits', {}).get('hits', [])
            
            doc_ids = [hit.get('_id') for hit in hits if hit.get('_id')]
            logger.debug('Found %d index documents for %d pks', len(doc_ids), len(pks))
            return doc_ids
        except Exception as e:
            logger.warning('Failed to query index IDs: %s', str(e))
            return []

    def delete_indexes_by_pks(self, index_name, pks):
        """Delete index documents by pk field"""
        if not pks:
            return
        
        doc_ids = self._query_index_ids_by_pks(index_name, pks)
        if not doc_ids:
            logger.debug('No index documents found to delete')
            return
        
        delete_params = []
        for _id in doc_ids:
            delete_params.append({'delete': {'_id': _id, '_index': index_name}})
            if len(delete_params) >= SEASEARCH_BULK_OPETATE_LIMIT:
                self.seasearch_api.bulk(index_name, delete_params)
                delete_params = []
        if delete_params:
            self.seasearch_api.bulk(index_name, delete_params)
        
        logger.info('Deleted %d index documents', len(doc_ids))

    def update(self, index_name, connection_id, project_uuid, connection_status, seaqa_db, last_ai_indexed_at):
        deleted_seadb_pk_set = set()

        index_count = 0
        page_size = 1000
        offset = 0
        bulk_params = []

        def _flush_bulk_if_needed(force=False):
            nonlocal bulk_params
            if bulk_params and (force or len(bulk_params) >= SEASEARCH_BULK_OPETATE_LIMIT):
                self.seasearch_api.bulk(index_name, bulk_params)
                bulk_params = []

        if not last_ai_indexed_at:
            last_ai_indexed_at = datetime.datetime(1970, 1, 1)
        
        last_ai_indexed_at = last_ai_indexed_at.replace(tzinfo=pytz.UTC).isoformat()
        table_name = DiscourseTopicsTable.gen_table_name(connection_id)
        
        while True:
            sql = f"""
                SELECT _pk, ai_summary, ai_summary_vector, deleted, ai_processed_time 
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
                
                if deleted:
                    deleted_seadb_pk_set.add(pk)
                    continue

                if not ai_summary or not ai_summary_vector:
                    logger.debug('Skip document without AI summary or vector, pk=%s', pk)
                    continue

                index_info = {"index": {"_index": index_name}}
                doc_info = {
                    "ai_summary": ai_summary,
                    "vec": ai_summary_vector,
                    "project_id": project_uuid,
                    "connection_id": connection_id,
                    "pk": pk,
                }
                bulk_params.append(index_info)
                bulk_params.append(doc_info)
                
                index_count += 1
                if need_update_connection_status(index_count):
                    connection_status['last_ai_indexed_count'] = index_count
                    seaqa_db.update_ai_index_status(connection_id, json.dumps(connection_status))
                
                _flush_bulk_if_needed()
            
            offset += page_size

        _flush_bulk_if_needed(force=True)
        
        if deleted_seadb_pk_set:
            self.delete_indexes_by_pks(index_name, deleted_seadb_pk_set)
            self.seadb_api.delete_rows(project_uuid, table_name, list(deleted_seadb_pk_set))
        
        return index_count

    def semantic_search(self, index_name, query_vector, top_k=10):
        data = {
            "query_field": "vec",
            "k": top_k,
            "return_fields": ["ai_summary", "project_id", "connection_id", "pk"],
            "vector": query_vector,
            "_source": False
        }

        search_results = self.seasearch_api.vector_search(
            index_name=index_name,
            data=data
        )

        hits = search_results.get('hits', {}).get('hits', [])
        hits.sort(key=lambda x: x.get('_score', 0), reverse=True)
        
        topk_search_results = []
        for i, hit in enumerate(hits):
            if i >= top_k:
                break
            fields = hit.get('fields', {})
            topk_search_results.append({
                'ai_summary': fields.get('ai_summary', [''])[0],
                'project_id': fields.get('project_id', [''])[0],
                'connection_id': fields.get('connection_id', [''])[0],
                'pk': fields.get('pk', [''])[0],
                'score': hit.get('_score', 0)
            })
        return topk_search_results

