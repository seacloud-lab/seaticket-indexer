import logging
import datetime

from seaqa_indexer.config import SEASEARCH_URL, SEASEARCH_TOKEN
from seaqa_indexer.seasearch.index_store.knowledge_base_index import KnowledgeBaseIndex
from seaqa_indexer.utils.seasearch_api import SeaSearchAPI
from seaqa_indexer.seasearch.utils import gen_kb_index_name
from seaqa_indexer.utils import time_to_utc_time
from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.seadb_models.knowledge_base_seadb_api import KnowledgeBaseSeaDBAPI


logger = logging.getLogger(__name__)


def update_knowledge_base_index(project_uuid, db_session_class):
    try:
        logger.info('Start to index knowledge base project_uuid %s', project_uuid)
        seasearch_api = SeaSearchAPI(SEASEARCH_URL, SEASEARCH_TOKEN)
        kb_index = KnowledgeBaseIndex(seasearch_api)
        seaqa_db = SeaqaDB(db_session_class)
        kb_seadb_api = KnowledgeBaseSeaDBAPI(project_uuid)

        index_name = gen_kb_index_name(project_uuid)
        kb_index.create_index_if_missing(index_name)
        project = seaqa_db.get_project_info_by_project_uuid(project_uuid)
        if not project:
            logger.warning('Project %s does not exist', project_uuid)
            return
        old_indexed_at = project.knowledge_base_indexed_at
        since = old_indexed_at
        if not since:
            since = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
        since = time_to_utc_time(str(since)).isoformat()

        indexed_count = kb_index.update(index_name, kb_seadb_api, since)
        indexed_at = datetime.datetime.now(datetime.UTC)
        seaqa_db.update_knowledge_base_indexed_at(project_uuid, indexed_at)
        logger.info('Finish to index knowledge base project_uuid %s', project_uuid)
    except Exception as e:
        logger.exception('update knowledge base: %s index failed, error: %s', project_uuid, e)