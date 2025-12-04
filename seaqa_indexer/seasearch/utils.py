import requests
import logging
import datetime

from seaqa_indexer.utils.constants import CONNECTION_TYPE_TO_INDEX_PREFIX
from seaqa_indexer.ai_processor.utils import generate_embedding


logger = logging.getLogger(__name__)

def gen_index_name_by_type(connection_id, connection_type):
    return CONNECTION_TYPE_TO_INDEX_PREFIX.get(connection_type) + str(connection_id)

def gen_summary_index_name(connection_id, connection_type):
    return f"{connection_type}_summary_vector_{connection_id}"

def gen_content_vector_index_name(connection_id, connection_type):
    return f"{connection_type}_content_vector_{connection_id}"

def get_content_by_download_link(download_url):
    response = requests.get(download_url)
    if response.status_code != 200:
        raise ConnectionError(response.status_code, response.text)
    file_content = response.content

    return file_content


def fix_summary_by_connection(connection_id, table_name, project_uuid, seadb_api):
    logger.info('Start fixing AI summary embeddings for connection %s', connection_id)

    try:
        page_size = 100
        fixed_count = 0
        updates = []
        while True:
            sql = f"""
                SELECT _pk, ai_summary
                FROM `{table_name}`
                WHERE ai_summary IS NOT NULL 
                AND ai_summary != ''
                AND ai_summary_vector IS NULL
                AND deleted = false
                LIMIT {page_size}
            """

            result = seadb_api.query_rows(project_uuid, sql)
            rows = result.get('results', [])

            if not rows:
                break

            for row in rows:
                pk = row.get('_pk')
                ai_summary = row.get('ai_summary')

                embedding = generate_embedding(ai_summary, project_uuid)
                if not embedding:
                    logger.warning('Failed to generate embedding for pk=%s', pk)
                    continue

                update_data = {
                    'pk': pk,
                    'row': {
                        'ai_summary_vector': embedding,
                        'ai_processed_time': datetime.datetime.now(datetime.UTC).isoformat()
                    }
                }

                updates.append(update_data)
                if len(updates) >= 50:
                    seadb_api.update_rows(project_uuid, table_name, updates)
                    updates = []

                fixed_count += 1
            if updates:
                seadb_api.update_rows(project_uuid, table_name, updates)

        logger.info('Finished fixing embeddings for connection %s, fixed %d records', connection_id, fixed_count)
        return fixed_count

    except Exception as e:
        logger.exception('Failed to fix embeddings for connection %s: %s', connection_id, e)
        return 0
