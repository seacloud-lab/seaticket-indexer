import logging
import datetime

from seaqa_indexer.seasearch.index_store.summary_vector_index import SummaryVectorIndex
from seaqa_indexer.seasearch.utils import gen_kb_summary_index_name
from seaqa_indexer.seadb_models.models import KnowledgeBaseTable


logger = logging.getLogger(__name__)


def update_knowledge_base_summary_index(knowledge_base_id, project_uuid, seaqa_db, seadb_api, seasearch_api):
    logger.info('Start to index knowledge base %s summary vectors for project %s', knowledge_base_id, project_uuid)

    kb = seaqa_db.get_kb_info_by_project_uuid(project_uuid)
    if not kb:
        logger.warning('Project %s does not exist', project_uuid)
        return

    last_kb_ai_indexed_at = kb.knowledge_base_ai_indexed_at

    table_name = KnowledgeBaseTable.gen_table_name()
    summary_vector_index = SummaryVectorIndex(seasearch_api, seadb_api)
    kb_summary_index_name = gen_kb_summary_index_name()

    summary_vector_index.create_index_if_missing(kb_summary_index_name)

    try:
        indexed_count = summary_vector_index.update_kb(
            table_name=table_name,
            index_name=kb_summary_index_name,
            project_uuid=project_uuid,
            last_ai_indexed_at=last_kb_ai_indexed_at,
        )

        ai_indexed_at = datetime.datetime.now(datetime.UTC)
        seaqa_db.update_knowledge_base_ai_indexed_at(project_uuid, ai_indexed_at)
        logger.info('Finish knowledge base summary vector indexing for project %s, indexed %d documents at %s',
                    project_uuid, indexed_count, ai_indexed_at)
    except Exception as e:
        logger.exception('Knowledge base %s summary vector indexing failed: %s', knowledge_base_id, e)
