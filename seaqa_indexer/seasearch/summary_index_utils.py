import logging
import json
import datetime

from seaqa_indexer.utils.constants import ConnectionType, IndexStatus
from seaqa_indexer.seasearch.index_store.summary_vector_index import SummaryVectorIndex

from seaqa_indexer.seasearch.utils import gen_conn_summary_index_name, fix_summary_by_connection, gen_kb_summary_index_name, fix_summary_by_knowledge_base
from seaqa_indexer.seadb_models.models import WebCrawlTable, DiscourseTopicsTable, GithubIssuesTable, SeafileTable, ThreadTable
from seaqa_indexer.seadb_models.models import KnowledgeBaseTable
from seaqa_indexer.seasearch.index_store.knowledge_base_summary_vector_index import KnowledgeBaseSummaryVectorIndex
from seaqa_indexer.seasearch.index_store.ticket_summary_vector_index import TicketSummaryVectorIndex
from seaqa_indexer.seasearch.utils import gen_ticket_summary_index_name, fix_summary_by_tickets
from seaqa_indexer.seadb_models.models import TicketsTable

logger = logging.getLogger(__name__)



def update_connection_summary_index(connection_id, seaqa_db, seadb_api, seasearch_api):
    logger.info('Start to index summary vectors for connection %s', connection_id)
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
            'last_ai_index_status': IndexStatus.PENDING,
            'last_ai_indexed_count': 0,
        }
    else:
        ai_status = json.loads(ai_status)

    # Set status to processing
    ai_status['last_ai_index_status'] = IndexStatus.INDEXING
    seaqa_db.update_ai_status_and_time(connection_id, json.dumps(ai_status))

    project_uuid = connection.project_uuid
    last_ai_indexed_at = connection.ai_indexed_at

    try:
        if connection_type == ConnectionType.SITE.value:
            table_name = WebCrawlTable.gen_table_name(connection_id)
        elif connection_type == ConnectionType.DISCOURSE_FORUM.value:
            table_name = DiscourseTopicsTable.gen_table_name(connection_id)
        elif connection_type == ConnectionType.GITHUB_ISSUE.value:
            table_name = GithubIssuesTable.gen_table_name(connection_id)
        elif connection_type == ConnectionType.SEAFILE.value:
            table_name = SeafileTable.gen_table_name(connection_id)
        elif connection_type == ConnectionType.EMAIL.value:
            table_name = ThreadTable.gen_table_name(connection_id)
        else:
            logger.warning('Unsupported connection type: %s for connection %s', connection_type, connection_id)
            ai_status['last_ai_index_status'] = IndexStatus.FAILED
            seaqa_db.update_ai_status_and_time(connection_id, json.dumps(ai_status))
            return

        fixed_count = fix_summary_by_connection(connection_id, table_name, project_uuid, seadb_api)
        if fixed_count > 0:
            logger.info('Fixed %d missing embeddings for connection %s before indexing', fixed_count, connection_id)

        summary_vector_index = SummaryVectorIndex(seasearch_api, seadb_api)
        ai_index_name = gen_conn_summary_index_name(connection_id, connection_type)
        summary_vector_index.create_index_if_missing(ai_index_name)

        index_count = summary_vector_index.update(
            table_name,
            ai_index_name,
            connection_id,
            project_uuid,
            ai_status,
            seaqa_db,
            last_ai_indexed_at,
        )

        ai_status['last_ai_indexed_count'] = index_count
        ai_status['last_ai_index_status'] = IndexStatus.COMPLETED
        ai_indexed_at = datetime.datetime.now(datetime.UTC)
        seaqa_db.update_ai_indexed_at_and_status(connection_id, ai_indexed_at, json.dumps(ai_status))

        logger.info('Finish summary vector indexing for connection %s, indexed %d documents', connection_id, index_count)
    except Exception as e:
        logger.exception('Connection %s update AI index failed, error: %s', connection_id, e)
        ai_status['last_ai_index_status'] = IndexStatus.FAILED
        seaqa_db.update_ai_status_and_time(connection_id, json.dumps(ai_status))


def update_knowledge_base_summary_index(project_uuid, seaqa_db, seadb_api, seasearch_api):
    logger.info('Start to index knowledge base summary vectors for project %s', project_uuid)

    kb = seaqa_db.get_kb_ai_info_by_project_uuid(project_uuid)
    if not kb:
        logger.warning('Project %s does not exist', project_uuid)
        return

    last_kb_ai_indexed_at = kb.knowledge_base_ai_indexed_at

    table_name = KnowledgeBaseTable.gen_table_name()
    fixed_count = fix_summary_by_knowledge_base(project_uuid, table_name, seadb_api)
    if fixed_count > 0:
        logger.info('Fixed %d missing embeddings for knowledge base of project: %s before indexing', fixed_count, project_uuid)

    kb_summary_vector_index = KnowledgeBaseSummaryVectorIndex(seasearch_api, seadb_api)
    kb_summary_index_name = gen_kb_summary_index_name(project_uuid)

    kb_summary_vector_index.create_index_if_missing(kb_summary_index_name)

    try:
        indexed_count = kb_summary_vector_index.update(
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
        logger.exception('Knowledge base summary vector of project %s indexing failed: %s', project_uuid, e)

def update_tickets_summary_index(project_uuid, seaqa_db, seadb_api, seasearch_api):
    logger.info('Start to index summary vectors for tickets of project: %s', project_uuid)

    ticket = seaqa_db.get_ticket_ai_info_by_project_uuid(project_uuid)
    if not ticket:
        logger.warning('Project %s does not exist', project_uuid)
        return

    last_ticket_ai_indexed_at = ticket.ticket_ai_indexed_at

    table_name = TicketsTable.gen_table_name()

    fixed_count = fix_summary_by_tickets(project_uuid, table_name, seadb_api)
    if fixed_count > 0:
        logger.info('Fixed %d missing embeddings for tickets of project: %s before indexing', fixed_count, project_uuid)

    ticket_summary_vector_index = TicketSummaryVectorIndex(seasearch_api, seadb_api)
    ai_index_name = gen_ticket_summary_index_name(project_uuid)
    ticket_summary_vector_index.create_index_if_missing(ai_index_name)

    index_count = ticket_summary_vector_index.update(
        table_name=table_name,
        index_name=ai_index_name,
        project_uuid=project_uuid,
        last_ai_indexed_at=last_ticket_ai_indexed_at,
    )
    return index_count
