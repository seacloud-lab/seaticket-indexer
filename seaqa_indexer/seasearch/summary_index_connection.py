import logging
import json
import datetime

from seaqa_indexer.utils.constants import ConnectionType, AIStatus
from seaqa_indexer.config import SEASEARCH_URL, SEASEARCH_TOKEN
from seaqa_indexer.seasearch.index_store.site_ai_vector_index import SiteAIVectorIndex
from seaqa_indexer.seasearch.index_store.discourse_ai_vector_index import DiscourseAIVectorIndex
from seaqa_indexer.seasearch.index_store.github_ai_vector_index import GitHubAIVectorIndex
from seaqa_indexer.seasearch.index_store.seafile_ai_vector_index import SeafileAIVectorIndex
from seaqa_indexer.utils.seasearch_api import SeaSearchAPI
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.seasearch.utils import gen_summary_index_name


logger = logging.getLogger(__name__)



def update_connection_summary_index(connection_id, db_session_class):
    logger.info('Start to index summary vectors for connection %s', connection_id)
    seaqa_db = SeaqaDB(db_session_class)
    connection = seaqa_db.get_project_connection_by_id(connection_id)
    
    if not connection:
        logger.warning('Connection %s does not exist', connection_id)
        return

    connection_is_active = connection.is_active
    if not connection_is_active:
        logger.warning('Connection %s is inactive', connection_id)
        return

    connection_type = connection.type
    ai_status = connection.ai_status
    
    if not ai_status:
        ai_status = {
            'last_ai_index_status': AIStatus.PENDING,
            'last_ai_indexed_count': 0,
        }
    else:
        ai_status = json.loads(ai_status)
    
    # Check if AI index is already in progress
    last_ai_index_status = ai_status.get('last_ai_index_status', AIStatus.PENDING)
    if last_ai_index_status == AIStatus.PROCESSING:
        logger.warning('Connection %s AI index is already processing', connection_id)
        return
    
    # Set status to processing
    ai_status['last_ai_index_status'] = AIStatus.PROCESSING
    seaqa_db.update_ai_status_and_time(connection_id, json.dumps(ai_status))

    project_uuid = connection.project_uuid
    last_ai_indexed_at = connection.ai_indexed_at
    
    try:
        from seaqa_indexer.seasearch.index_manager import IndexManager
        index_manager = IndexManager()
        fixed_count = index_manager.fix_summary_by_connection(connection_id, connection_type, project_uuid)
        if fixed_count > 0:
            logger.info('Fixed %d missing embeddings for connection %s before indexing', fixed_count, connection_id)
        
        seasearch_api = SeaSearchAPI(SEASEARCH_URL, SEASEARCH_TOKEN)
        seadb_api = SeaDBAPI()
        if connection_type == ConnectionType.SITE.value:
            ai_index = SiteAIVectorIndex(seasearch_api, seadb_api)
        elif connection_type == ConnectionType.DISCOURSE_FORUM.value:
            ai_index = DiscourseAIVectorIndex(seasearch_api, seadb_api)
        elif connection_type == ConnectionType.GITHUB_ISSUE.value:
            ai_index = GitHubAIVectorIndex(seasearch_api, seadb_api)
        elif connection_type == ConnectionType.SEAFILE.value:
            ai_index = SeafileAIVectorIndex(seasearch_api, seadb_api)
        else:
            logger.warning('Unsupported connection type: %s for connection %s', connection_type, connection_id)
            ai_status['last_ai_index_status'] = AIStatus.FAILED
            seaqa_db.update_ai_status_and_time(connection_id, json.dumps(ai_status))
            return
        
        ai_index_name = gen_summary_index_name(connection_id, connection_type)
        ai_index.create_index_if_missing(ai_index_name)

        index_count = ai_index.update(
            ai_index_name, 
            connection_id, 
            project_uuid, 
            ai_status, 
            seaqa_db, 
            last_ai_indexed_at
        )
        
        ai_status['last_ai_indexed_count'] = index_count
        ai_status['last_ai_index_status'] = AIStatus.COMPLETED
        ai_indexed_at = datetime.datetime.now(datetime.UTC)
        seaqa_db.update_ai_indexed_at_and_status(connection_id, ai_indexed_at, json.dumps(ai_status))
            
        logger.info('Finish summary vector indexing for connection %s, indexed %d documents', connection_id, index_count)
    except Exception as e:
        logger.exception('Connection %s update AI index failed, error: %s', connection_id, e)
        ai_status['last_ai_index_status'] = AIStatus.FAILED
        last_ai_indexed_time = datetime.datetime.now(datetime.UTC)
        seaqa_db.update_ai_status_and_time(connection_id, json.dumps(ai_status), last_ai_indexed_time)
