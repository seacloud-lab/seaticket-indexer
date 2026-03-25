import logging
import json

from seaqa_indexer.utils import need_update_connection_status
from seaqa_indexer.crawler.utils import url_to_filename
from seaqa_indexer.ai_processor.utils import generate_chunks_embedding
from seaqa_indexer.index.index_store.base_vector_index import BaseVectorIndex
from seaqa_indexer.utils.constants import SEASEARCH_BULK_OPERATE_LIMIT, ConnectionType, AIScenario

logger = logging.getLogger(__name__)


class ContentVectorIndex(BaseVectorIndex):
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

    def update(self, table_name, index_name, connection_id, project_uuid,
               content_vector_status, seaqa_db, seadb_api, storage, since, connection_type):

        index_count = 0
        page_size = 100  # Smaller page size since we process chunks
        offset = 0
        deleted_index_pk_set = set()
        bulk_params = []

        while True:
            if connection_type == ConnectionType.SITE.value:
                sql = f"""
                    SELECT _pk, url, deleted, outdated, sync_time, modified_time 
                    FROM `{table_name}` 
                    WHERE record_modified_time >= '{since}'
                    ORDER BY sync_time 
                    LIMIT {page_size} OFFSET {offset}
                """

                def get_content(row):
                    url = row.get('url')
                    filename = url_to_filename(url)
                    file_json = storage.read_json(connection_id, project_uuid, filename)
                    return (file_json or {}).get('content')

            elif connection_type == ConnectionType.SEAFILE.value:
                sql = f"""
                    SELECT _pk, content, deleted, outdated, sync_time, modified_time
                    FROM `{table_name}`
                    WHERE record_modified_time >= '{since}'
                    ORDER BY sync_time
                    LIMIT {page_size} OFFSET {offset}
                """

                def get_content(row):
                    return row.get('content')

            else:
                return index_count

            result = seadb_api.query_rows(project_uuid, sql)
            rows = result.get('results', [])
            if not rows:
                break

            for row in rows:
                pk = row.get('_pk')
                deleted = row.get('deleted')
                outdated = row.get('outdated')
                modified_time = row.get('modified_time')

                if deleted or outdated:
                    deleted_index_pk_set.add(pk)
                    continue

                content = get_content(row)
                if not content:
                    logger.debug('Skip empty content document pk=%s', pk)
                    continue

                # Chunk the content
                chunks, embeddings = generate_chunks_embedding(content, project_uuid, scenario=AIScenario.SUMMARY.value)
                # Delete old chunks for this document before adding new ones
                self.delete_indexes_by_pks(index_name, [pk])

                # Index each chunk
                segment_count = min(len(chunks), len(embeddings))
                for i in range(segment_count):
                    index_info = {"index": {"_index": index_name}}
                    doc_info = {
                        "content": chunks[i],
                        "vec": embeddings[i].get('embedding', []),
                        "project_uuid": project_uuid,
                        "connection_id": str(connection_id),
                        "pk": str(pk),
                        "chunk_index": embeddings[i].get('index'),
                        "modified_time": modified_time
                    }
                    bulk_params.append(index_info)
                    bulk_params.append(doc_info)

                    index_count += 1
                    if need_update_connection_status(index_count):
                        content_vector_status['last_content_vector_indexed_count'] = index_count
                        seaqa_db.update_content_vector_status_and_time(connection_id, json.dumps(content_vector_status))

                    if len(bulk_params) >= SEASEARCH_BULK_OPERATE_LIMIT:
                        self.seasearch_api.bulk(index_name, bulk_params)
                        bulk_params = []

            offset += page_size

        if bulk_params:
            self.seasearch_api.bulk(index_name, bulk_params)

        if deleted_index_pk_set:
            self.delete_indexes_by_pks(index_name, deleted_index_pk_set)
        return index_count


    def semantic_search(self, index_names, query_vector, top_k=10, time_from=None, time_to=None):
        data = {
            "query_field": "vec",
            "k": top_k,
            "return_fields": ["content", "project_uuid", "connection_id", "pk", "chunk_index", "modified_time"],
            "vector": query_vector,
            "_source": False
        }

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
                'content': fields.get('content', [''])[0],
                'project_uuid': fields.get('project_uuid')[0],
                'connection_id': fields.get('connection_id')[0],
                'pk': fields.get('pk')[0],
                'chunk_index': fields.get('chunk_index', [0])[0],
                'modified_time': fields.get('modified_time', [None])[0],
                'score': hit.get('_score', 0),
                'source_type': 'chunk',
            })

        return topk_search_results
