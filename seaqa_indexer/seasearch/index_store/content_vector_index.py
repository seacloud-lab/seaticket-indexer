import logging
import json
import datetime
import pytz

from seaqa_indexer.utils import need_update_connection_status
from seaqa_indexer.crawler.utils import url_to_filename
from seaqa_indexer.ai_processor.utils import generate_chunks_embedding


logger = logging.getLogger(__name__)

SEASEARCH_BULK_OPERATE_LIMIT = 100


class ContentVectorIndex(object):
    mapping = {
        'properties': {
            'content': {
                'type': 'text',
            },
            'project_uuid': {
                'type': 'keyword'
            },
            'connection_id': {
                'type': 'keyword'
            },
            'pk': {
                'type': 'keyword'
            },
            'chunk_index': {
                'type': 'integer'
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
            },
        }
    }

    def __init__(self, seasearch_api, seadb_api, storage=None, shard_num=1):
        self.seasearch_api = seasearch_api
        self.seadb_api = seadb_api
        self.storage = storage
        self.shard_num = shard_num

    def create_index_if_missing(self, index_name):
        if not self.seasearch_api.check_index_mapping(index_name).get('is_exist'):
            self.seasearch_api.create_index(
                index_name,
                {"shard_num": self.shard_num, "mappings": self.mapping}
            )

    def check_index(self, index_name):
        return self.seasearch_api.check_index_mapping(index_name).get('is_exist')
    
    def delete_index_by_index_name(self, index_name):
        self.seasearch_api.delete_index_by_name(index_name)

    def _query_index_ids_by_pk(self, index_name, pk):
        """Query index document IDs by pk and url fields"""
        try:
            query = {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"pk": str(pk)}},
                        ]
                    }
                },
                "_source": False,
                "size": 1000  # Assume max 1000 chunks per document
            }
            result = self.seasearch_api.normal_search(index_name, query)
            hits = result.get('hits', {}).get('hits', [])

            doc_ids = [hit.get('_id') for hit in hits if hit.get('_id')]
            logger.debug('Found %d index documents for pk=%s', len(doc_ids), pk)
            return doc_ids
        except Exception as e:
            logger.warning('Failed to query index IDs: %s', str(e))
            return []

    def delete_indexes_by_pk(self, index_name, pk):
        """Delete index documents by pk and url fields"""
        doc_ids = self._query_index_ids_by_pk(index_name, pk)
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

        logger.debug('Deleted %d chunk index documents for pk=%s', len(doc_ids), pk)

    def update(self, table_name, index_name, connection_id, project_uuid,
               content_vector_status, seaqa_db, last_content_vector_indexed_at):

        index_count = 0
        page_size = 100  # Smaller page size since we process chunks
        offset = 0
        bulk_params = []

        def _flush_bulk_if_needed(force=False):
            nonlocal bulk_params
            if bulk_params and (force or len(bulk_params) >= SEASEARCH_BULK_OPERATE_LIMIT):
                self.seasearch_api.bulk(index_name, bulk_params)
                bulk_params = []

        if not last_content_vector_indexed_at:
            last_content_vector_indexed_at = datetime.datetime(1970, 1, 1)

        last_content_vector_indexed_at = last_content_vector_indexed_at.replace(tzinfo=pytz.UTC).isoformat()

        while True:
            sql = f"""
                SELECT _pk, url, deleted, sync_time, modified_time 
                FROM `{table_name}` 
                WHERE sync_time > '{last_content_vector_indexed_at}'
                ORDER BY sync_time 
                LIMIT {page_size} OFFSET {offset}
            """
            result = self.seadb_api.query_rows(project_uuid, sql)
            rows = result.get('results', [])
            if not rows:
                break

            for row in rows:
                pk = row.get('_pk')
                url = row.get('url')
                deleted = row.get('deleted')
                modified_time = row.get('modified_time')

                if deleted:
                    continue

                filename = url_to_filename(url)
                file_json = self.storage.read_json(connection_id, project_uuid, filename)
                content = (file_json or {}).get('content')

                if not content:
                    logger.debug('Skip empty content document url=%s', url)
                    continue

                # Chunk the content
                chunks, embeddings = generate_chunks_embedding(content, project_uuid)
                # Delete old chunks for this document before adding new ones
                self.delete_indexes_by_pk(index_name, pk)

                # Index each chunk
                segment_count = min(len(chunks), len(embeddings))
                for i in range(segment_count):
                    index_info = {"index": {"_index": index_name}}
                    doc_info = {
                        "content": chunks[i],
                        "vec": embeddings[i].get('embedding', []),
                        "project_uuid": project_uuid,
                        "connection_id": connection_id,
                        "pk": pk,
                        "chunk_index": embeddings[i].get('index'),
                        "modified_time": modified_time
                    }
                    bulk_params.append(index_info)
                    bulk_params.append(doc_info)

                    index_count += 1
                    if need_update_connection_status(index_count):
                        content_vector_status['last_content_vector_indexed_count'] = index_count
                        seaqa_db.update_content_vector_status_and_time(connection_id, json.dumps(content_vector_status))

                    _flush_bulk_if_needed()

            offset += page_size

        _flush_bulk_if_needed(force=True)
        return index_count

    def semantic_search(self, index_names, query_vector, top_k=10, connection_id=None, time_from=None, time_to=None):

        if not query_vector:
            return []

        try:
            data = {
                "query_field": "vec",
                "k": top_k,
                "return_fields": ["content", "project_uuid", "connection_id", "pk", "chunk_index", "modified_time"],
                "vector": query_vector,
                "_source": False
            }

            filters = []

            if connection_id:
                filters.append({
                    "term": {
                        "connection_id": connection_id
                    }
                })

            if time_from is not None or time_to is not None:
                range_filter = {}
                if time_from is not None:
                    range_filter["gte"] = time_from
                if time_to is not None:
                    range_filter["lte"] = time_to

                filters.append({
                    "range": {
                        "modified_time": range_filter
                    }
                })

            if filters:
                if len(filters) == 1:
                    data["filter"] = filters[0]
                else:
                    data["filter"] = {
                        "bool": {
                            "must": filters
                        }
                    }
            search_results = self.seasearch_api.vector_search(
                index_names=index_names,
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
                    'content': fields.get('content', [''])[0],
                    'project_uuid': fields.get('project_uuid')[0],
                    'connection_id': fields.get('connection_id')[0],
                    'pk': fields.get('pk')[0],
                    'chunk_index': fields.get('chunk_index', [0])[0],
                    'modified_time': fields.get('modified_time', [None])[0],
                    'score': hit.get('_score', 0)
                })

            return topk_search_results
        except Exception as e:
            logger.exception('Semantic search failed: %s', e)
            return []
