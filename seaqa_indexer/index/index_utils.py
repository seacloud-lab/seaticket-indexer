import logging
import json
import datetime

from seaqa_indexer.utils.constants import IndexStatus, SUPPORT_VECTOR_RETRIEVAL_TYPES
from seaqa_indexer.config import SEASEARCH_URL, SEASEARCH_TOKEN
from seaqa_indexer.index.index_store.knowledge_base_index import KnowledgeBaseIndex
from seaqa_indexer.utils.seasearch_api import SeaSearchAPI
from seaqa_indexer.utils import uuid_str_to_32_chars
from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.seadb_models.knowledge_base_seadb_api import KnowledgeBaseSeaDBAPI
from seaqa_indexer.seadb_models.ticket_seadb_api import TicketSeaDBAPI
from seaqa_indexer.seadb_models.portal_issue_seadb_api import PortalIssueSeaDBAPI
from seaqa_indexer.index.index_store.ticket_index import TicketIndex
from seaqa_indexer.index.index_store.portal_issue_index import PortalIssueIndex
from seaqa_indexer.index.utils import fix_summary, gen_kb_summary_index_name, gen_ticket_index_name,\
    gen_ticket_summary_index_name, gen_kb_index_name, gen_portal_issue_index_name, gen_portal_issue_summary_index_name
from seaqa_indexer.seadb_models.utils import get_table_name_from_schema
from seaqa_indexer.index.index_store.knowledge_base_summary_vector_index import KnowledgeBaseSummaryVectorIndex
from seaqa_indexer.index.index_store.ticket_summary_vector_index import TicketSummaryVectorIndex
from seaqa_indexer.index.index_store.portal_issue_summary_vector_index import PortalIssueSummaryVectorIndex
from seaqa_indexer.handlers import get_handler
from seaqa_indexer.utils import get_since_time
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.crawler.storage import get_storage

from seaqa_indexer.seadb_models.models import SchemaTableNames



logger = logging.getLogger(__name__)


def update_connection_index(connection_id, db_session_class, is_rebuild=False):
    logger.info('Start to index project connection %s', connection_id)
    seaqa_db = SeaqaDB(db_session_class)
    connection = seaqa_db.get_project_connection_by_id(connection_id)
    if not connection:
        logger.warning('Project connection %s does not exist', connection_id)
        return

    connection_is_active = connection.is_active
    if not connection_is_active:
        logger.warning('Connection %s is inactive', connection_id)
        return

    connection_type = connection.type
    connection_status = json.loads(connection.status)

    connection_status['last_index_status'] = IndexStatus.INDEXING
    seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))

    old_indexed_at = connection.indexed_at
    since = get_since_time(old_indexed_at, is_rebuild)

    seasearch_api = SeaSearchAPI(SEASEARCH_URL, SEASEARCH_TOKEN)
    try:
        conn_handler = get_handler(connection_type)
        if not conn_handler:
            logger.warning('Unsupported connection type: %s for connection %s', connection_type, connection_id)
            return
        conn_handler.update_index(connection, seasearch_api, seaqa_db, since)
    except Exception as e:
        logger.exception('Connection %s update index failed, error: %s', connection_id, e)
        connection_status['last_index_status'] = IndexStatus.FAILED
        seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))

    logger.info('Finish index project connection %s, connection_type is %s', connection_id, connection_type)

def update_knowledge_base_index(project_uuid, db_session_class, is_rebuild=False):
    try:
        logger.info('Start to index knowledge base project_uuid %s', project_uuid)
        seasearch_api = SeaSearchAPI(SEASEARCH_URL, SEASEARCH_TOKEN)
        kb_index = KnowledgeBaseIndex(seasearch_api)
        seaqa_db = SeaqaDB(db_session_class)
        kb_seadb_api = KnowledgeBaseSeaDBAPI(project_uuid)

        project = seaqa_db.get_project_info_by_project_uuid(project_uuid)
        if not project:
            logger.warning('Project %s does not exist', project_uuid)
            return
        index_name = gen_kb_index_name(project_uuid)
        kb_index.create_index_if_missing(index_name)
        old_indexed_at = project.knowledge_base_indexed_at
        since = get_since_time(old_indexed_at, is_rebuild)

        kb_index.update(index_name, uuid_str_to_32_chars(project_uuid), kb_seadb_api, since)
        indexed_at = datetime.datetime.now(datetime.UTC)
        seaqa_db.update_knowledge_base_indexed_at(project_uuid, indexed_at)
        logger.info('Finish to index knowledge base project_uuid %s', project_uuid)
    except Exception as e:
        logger.exception('update knowledge base: %s index failed, error: %s', project_uuid, e)


def update_ticket_index(project_uuid, db_session_class, is_rebuild=False):
    try:
        logger.info('Start to index ticket project_uuid %s', project_uuid)
        seasearch_api = SeaSearchAPI(SEASEARCH_URL, SEASEARCH_TOKEN)
        ticket_index = TicketIndex(seasearch_api)
        seaqa_db = SeaqaDB(db_session_class)
        ticket_seadb_api = TicketSeaDBAPI(project_uuid)

        project = seaqa_db.get_ticket_indexed_at_by_project_uuid(project_uuid)
        if not project:
            logger.warning('Project %s does not exist', project_uuid)
            return
        index_name = gen_ticket_index_name(project_uuid)
        ticket_index.create_index_if_missing(index_name)
        old_indexed_at = project.ticket_indexed_at
        since = get_since_time(old_indexed_at, is_rebuild)

        ticket_index.update(index_name, uuid_str_to_32_chars(project_uuid), ticket_seadb_api, since)
        indexed_at = datetime.datetime.now(datetime.UTC)
        seaqa_db.update_ticket_indexed_at(project_uuid, indexed_at)
        logger.info('Finish to index ticket project_uuid %s', project_uuid)
    except Exception as e:
        logger.exception('update ticket: %s index failed, error: %s', project_uuid, e)


def update_portal_issue_index(project_uuid, db_session_class, is_rebuild=False):
    try:
        logger.info('Start to index portal issue project_uuid %s', project_uuid)
        seasearch_api = SeaSearchAPI(SEASEARCH_URL, SEASEARCH_TOKEN)
        portal_issue_index = PortalIssueIndex(seasearch_api)
        seaqa_db = SeaqaDB(db_session_class)
        portal_issue_seadb_api = PortalIssueSeaDBAPI(project_uuid)

        project = seaqa_db.get_portal_issue_indexed_at_by_project_uuid(project_uuid)
        if not project:
            logger.warning('Project %s does not exist', project_uuid)
            return
        index_name = gen_portal_issue_index_name(project_uuid)
        portal_issue_index.create_index_if_missing(index_name)
        old_indexed_at = project.portal_issue_indexed_at
        since = get_since_time(old_indexed_at, is_rebuild)

        portal_issue_index.update(index_name, uuid_str_to_32_chars(project_uuid), portal_issue_seadb_api, since)
        indexed_at = datetime.datetime.now(datetime.UTC)
        seaqa_db.update_portal_issue_indexed_at(project_uuid, indexed_at)
        logger.info('Finish to index portal issue project_uuid %s', project_uuid)
    except Exception as e:
        logger.exception('update portal issue: %s index failed, error: %s', project_uuid, e)


def update_connection_content_vector_index(connection_id, db_session_class, is_rebuild=False):
    seadb_api = SeaDBAPI()
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
    content_vector_status = connection.content_vector_status

    if not content_vector_status:
        content_vector_status = {
            'last_content_vector_index_status': IndexStatus.PENDING,
            'last_content_vector_indexed_count': 0,
        }
    else:
        content_vector_status = json.loads(content_vector_status)

    content_vector_status['last_content_vector_index_status'] = IndexStatus.INDEXING
    seaqa_db.update_content_vector_status_and_time(connection_id, json.dumps(content_vector_status))

    project_uuid = connection.project_uuid
    last_content_vector_indexed_at = connection.content_vector_indexed_at
    since = get_since_time(last_content_vector_indexed_at, is_rebuild)

    try:
        if connection_type not in SUPPORT_VECTOR_RETRIEVAL_TYPES:
            logger.warning('Unsupported connection type: %s for connection %s', connection_type, connection_id)
            return

        handler = get_handler(connection_type)
        if not handler:
            logger.warning('Unsupported connection type: %s for connection %s', connection_type, connection_id)
            return

        storage = get_storage()
        seasearch_api = SeaSearchAPI(SEASEARCH_URL, SEASEARCH_TOKEN)
        handler.update_content_vector_index(project_uuid, connection_id, content_vector_status, seaqa_db, seadb_api, storage, seasearch_api, since)
    except Exception as e:
        logger.exception('Connection %s update content vector index failed, error: %s', connection_id, e)
        content_vector_status['last_content_vector_index_status'] = IndexStatus.FAILED
        seaqa_db.update_content_vector_status_and_time(
            connection_id,
            json.dumps(content_vector_status)
        )


def update_connection_summary_index(connection_id, db_session_class, is_rebuild=False):
    logger.info('Start to index summary vectors for connection %s', connection_id)

    seadb_api = SeaDBAPI()
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

    since = get_since_time(last_ai_indexed_at, is_rebuild)

    try:
        handler = get_handler(connection_type)
        if not handler:
            logger.warning('Unsupported connection type: %s for connection %s', connection_type, connection_id)
            return
        seasearch_api = SeaSearchAPI(SEASEARCH_URL, SEASEARCH_TOKEN)
        handler.update_summary_index(project_uuid, connection_id, seadb_api, ai_status, seaqa_db, seasearch_api, since)
    except Exception as e:
        logger.exception('Connection %s update AI index failed, error: %s', connection_id, e)
        ai_status['last_ai_index_status'] = IndexStatus.FAILED
        seaqa_db.update_ai_status_and_time(connection_id, json.dumps(ai_status))


def update_knowledge_base_summary_index(project_uuid, db_session_class, is_rebuild=False):
    logger.info('Start to index knowledge base summary vectors for project %s', project_uuid)

    seadb_api = SeaDBAPI()
    seaqa_db = SeaqaDB(db_session_class)

    kb = seaqa_db.get_kb_ai_info_by_project_uuid(project_uuid)
    if not kb:
        logger.warning('Project %s does not exist', project_uuid)
        return

    last_kb_ai_indexed_at = kb.knowledge_base_ai_indexed_at

    table_name = get_table_name_from_schema(SchemaTableNames.KNOWLEDGE_BASE)
    fixed_count = fix_summary(project_uuid, table_name, seadb_api)
    if fixed_count > 0:
        logger.info('Fixed %d missing embeddings for knowledge base of project: %s before indexing', fixed_count, project_uuid)

    seasearch_api = SeaSearchAPI(SEASEARCH_URL, SEASEARCH_TOKEN)
    kb_summary_vector_index = KnowledgeBaseSummaryVectorIndex(seasearch_api)
    index_name = gen_kb_summary_index_name(project_uuid)

    kb_summary_vector_index.create_index_if_missing(index_name)

    since = get_since_time(last_kb_ai_indexed_at, is_rebuild)
    try:
        indexed_count = kb_summary_vector_index.update(table_name, index_name, project_uuid, seadb_api, since)
        ai_indexed_at = datetime.datetime.now(datetime.UTC)
        seaqa_db.update_knowledge_base_ai_indexed_at(project_uuid, ai_indexed_at)
        logger.info('Finish knowledge base summary vector indexing for project %s, indexed %d documents at %s',
                    project_uuid, indexed_count, ai_indexed_at)
    except Exception as e:
        logger.exception('Knowledge base summary vector of project %s indexing failed: %s', project_uuid, e)


def update_tickets_summary_index(project_uuid, db_session_class, is_rebuild=False):
    logger.info('Start to index summary vectors for tickets of project: %s', project_uuid)
    seadb_api = SeaDBAPI()
    seaqa_db = SeaqaDB(db_session_class)

    ticket = seaqa_db.get_ticket_ai_info_by_project_uuid(project_uuid)
    if not ticket:
        logger.warning('Project %s does not exist', project_uuid)
        return

    last_ticket_ai_indexed_at = ticket.ticket_ai_indexed_at

    table_name = get_table_name_from_schema(SchemaTableNames.TICKETS)

    fixed_count = fix_summary(project_uuid, table_name, seadb_api)
    if fixed_count > 0:
        logger.info('Fixed %d missing embeddings for tickets of project: %s before indexing', fixed_count, project_uuid)

    seasearch_api = SeaSearchAPI(SEASEARCH_URL, SEASEARCH_TOKEN)
    ticket_summary_vector_index = TicketSummaryVectorIndex(seasearch_api)
    ai_index_name = gen_ticket_summary_index_name(project_uuid)
    ticket_summary_vector_index.create_index_if_missing(ai_index_name)

    since = get_since_time(last_ticket_ai_indexed_at, is_rebuild)

    index_count = ticket_summary_vector_index.update(table_name, ai_index_name, project_uuid, seadb_api, since)
    return index_count


def update_portal_issues_summary_index(project_uuid, db_session_class, is_rebuild=False):
    logger.info('Start to index summary vectors for portal issues of project: %s', project_uuid)
    seadb_api = SeaDBAPI()
    seaqa_db = SeaqaDB(db_session_class)

    portal_issue = seaqa_db.get_portal_issue_ai_info_by_project_uuid(project_uuid)
    if not portal_issue:
        logger.warning('Project %s does not exist', project_uuid)
        return

    last_portal_issue_ai_indexed_at = portal_issue.portal_issue_ai_indexed_at

    table_name = get_table_name_from_schema(SchemaTableNames.PORTAL_ISSUES)

    fixed_count = fix_summary(project_uuid, table_name, seadb_api)
    if fixed_count > 0:
        logger.info('Fixed %d missing embeddings for portal issues of project: %s before indexing', fixed_count, project_uuid)

    seasearch_api = SeaSearchAPI(SEASEARCH_URL, SEASEARCH_TOKEN)
    portal_issue_summary_vector_index = PortalIssueSummaryVectorIndex(seasearch_api)
    ai_index_name = gen_portal_issue_summary_index_name(project_uuid)
    portal_issue_summary_vector_index.create_index_if_missing(ai_index_name)

    since = get_since_time(last_portal_issue_ai_indexed_at, is_rebuild)

    indexed_count = portal_issue_summary_vector_index.update(table_name, ai_index_name, project_uuid, seadb_api, since)
    ai_indexed_at = datetime.datetime.now(datetime.UTC)
    seaqa_db.update_portal_issue_ai_indexed_at(project_uuid, ai_indexed_at)
    logger.info('Finish summary vector indexing for portal issues of project %s, indexed %d documents at %s',
                project_uuid, indexed_count, ai_indexed_at)
    return indexed_count
