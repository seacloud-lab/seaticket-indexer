import logging
import json
import datetime

from seaqa_indexer.utils.constants import ConnectionType, IndexStatus
from seaqa_indexer.seasearch.index_store.content_vector_index import ContentVectorIndex
from seaqa_indexer.seasearch.utils import gen_content_vector_index_name
from seaqa_indexer.seadb_models.models import WebCrawlTable


logger = logging.getLogger(__name__)


def update_connection_content_vector_index(connection_id, seaqa_db, seadb_api, storage, seasearch_api):
    connection = seaqa_db.get_project_connection_by_id(connection_id)
    
    if not connection:
        logger.warning('Connection %s does not exist', connection_id)
        return

    connection_is_active = connection.is_active
    if not connection_is_active:
        logger.warning('Connection %s is inactive', connection_id)
        return

    connection_type = connection.type
    content_vector_status = connection.content_vector_status
    
    if not content_vector_status:
        content_vector_status = {
            'last_content_vector_index_status': IndexStatus.PENDING,
            'last_content_vector_indexed_count': 0,
        }
    else:
        content_vector_status = json.loads(content_vector_status)
    
    last_status = content_vector_status.get('last_content_vector_index_status', IndexStatus.PENDING)
    if last_status == IndexStatus.INDEXING:
        logger.warning('Connection %s content vector index is already processing', connection_id)
        return
    
    content_vector_status['last_content_vector_index_status'] = IndexStatus.INDEXING
    seaqa_db.update_content_vector_status_and_time(connection_id, json.dumps(content_vector_status))

    project_uuid = connection.project_uuid
    last_content_vector_indexed_at = connection.content_vector_indexed_at
    
    try:
        # Only support SITE type for now
        if connection_type != ConnectionType.SITE.value:
            logger.warning('Unsupported connection type: %s for connection %s', connection_type, connection_id)
            content_vector_status['last_content_vector_index_status'] = IndexStatus.FAILED
            seaqa_db.update_content_vector_status_and_time(connection_id, json.dumps(content_vector_status))
            return
                
        table_name = WebCrawlTable.gen_table_name(connection_id)
        content_vector_index = ContentVectorIndex(seasearch_api, seadb_api, storage)
        content_index_name = gen_content_vector_index_name(connection_id, connection_type)
        content_vector_index.create_index_if_missing(content_index_name)

        index_count = content_vector_index.update(
            table_name,
            content_index_name, 
            connection_id, 
            project_uuid, 
            content_vector_status, 
            seaqa_db, 
            last_content_vector_indexed_at
        )
        
        content_vector_status['last_content_vector_indexed_count'] = index_count
        content_vector_status['last_content_vector_index_status'] = IndexStatus.COMPLETED
        content_vector_indexed_at = datetime.datetime.now(datetime.UTC)
        seaqa_db.update_content_vector_status_and_time(
            connection_id, 
            json.dumps(content_vector_status),
            content_vector_indexed_at
        )
            
        logger.info('Finish content vector indexing for connection %s, indexed %d chunks', connection_id, index_count)
    except Exception as e:
        logger.exception('Connection %s update content vector index failed, error: %s', connection_id, e)
        content_vector_status['last_content_vector_index_status'] = IndexStatus.FAILED
        seaqa_db.update_content_vector_status_and_time(
            connection_id, 
            json.dumps(content_vector_status)
        )
