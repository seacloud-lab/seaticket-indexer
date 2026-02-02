import logging
import json
import datetime
from urllib.parse import urlparse

from seaqa_indexer.seadb_models.github_seadb_api import GitHubSeaDBAPI
from seaqa_indexer.utils.constants import ConnectionType, IndexStatus
from seaqa_indexer.config import SEASEARCH_URL, SEASEARCH_TOKEN
from seaqa_indexer.index.index_store.site_index import SiteIndex
from seaqa_indexer.index.index_store.seafile_index import SeafileIndex
from seaqa_indexer.index.index_store.github_issues_index import GitHubIssuesIndex
from seaqa_indexer.index.index_store.discourse_index import DiscourseIndex
from seaqa_indexer.index.index_store.email_index import EmailIndex
from seaqa_indexer.index.index_store.knowledge_base_index import KnowledgeBaseIndex
from seaqa_indexer.utils.seasearch_api import SeaSearchAPI
from seaqa_indexer.utils import time_to_utc_time, decrypt_config, uuid_str_to_32_chars
from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.seadb_models.discourse_seadb_api import DiscourseSeaDBAPI
from seaqa_indexer.seadb_models.seafile_seadb_api import SeafileSeaDBAPI
from seaqa_indexer.seadb_models.email_seadb_api import EmailSeaDBAPI
from seaqa_indexer.seadb_models.knowledge_base_seadb_api import KnowledgeBaseSeaDBAPI
from seaqa_indexer.seadb_models.ticket_seadb_api import TicketSeaDBAPI
from seaqa_indexer.index.index_store.ticket_index import TicketIndex
from seaqa_indexer.crawler.storage import get_storage
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.index.index_store.content_vector_index import ContentVectorIndex

from seaqa_indexer.index.index_store.summary_vector_index import SummaryVectorIndex
from seaqa_indexer.index.utils import gen_conn_summary_index_name, fix_summary, \
    gen_kb_summary_index_name, gen_ticket_index_name, gen_content_vector_index_name, \
    gen_ticket_summary_index_name, gen_index_name_by_type, gen_kb_index_name
from seaqa_indexer.seadb_models.models import KnowledgeBaseTable, TicketsTable
from seaqa_indexer.index.index_store.knowledge_base_summary_vector_index import KnowledgeBaseSummaryVectorIndex
from seaqa_indexer.index.index_store.ticket_summary_vector_index import TicketSummaryVectorIndex
from seaqa_indexer.handlers import get_handler, get_handler


logger = logging.getLogger(__name__)


def get_since_time(old_indexed_at) -> str:
    if not old_indexed_at:
        return datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc).isoformat()
    return time_to_utc_time(str(old_indexed_at)).isoformat()

def update_connection_index(connection_id, db_session_class):
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

    seasearch_api = SeaSearchAPI(SEASEARCH_URL, SEASEARCH_TOKEN)
    try:
        conn_handler = get_handler(connection_type)
        if not conn_handler:
            logger.warning('Unsupported connection type: %s for connection %s', connection_type, connection_id)
            return
        conn_handler.update_index(connection, seasearch_api, seaqa_db)
    except Exception as e:
        logger.exception('Connection %s update index failed, error: %s', connection_id, e)
        connection_status['last_index_status'] = IndexStatus.FAILED
        seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))

    logger.info('Finish index project connection %s, connection_type is %s', connection_id, connection_type)


def update_site_index(project_connection, seasearch_api, seaqa_db):
    connection_status = json.loads(project_connection.status)
    site_index = SiteIndex(seasearch_api)

    connection_id = project_connection.id
    project_uuid = project_connection.project_uuid
    connection_type = project_connection.type
    old_indexed_at = project_connection.indexed_at

    site_index_name = gen_index_name_by_type(connection_id, connection_type)
    site_index.create_index_if_missing(site_index_name)

    seadb_api = SeaDBAPI()
    storage = get_storage()

    since = get_since_time(old_indexed_at)

    index_count = site_index.update(site_index_name, connection_id, project_uuid, connection_status, seaqa_db, seadb_api, storage, since)
    connection_status['last_indexed_count'] = index_count
    connection_status['last_index_status'] = IndexStatus.COMPLETED
    indexed_at = datetime.datetime.now(datetime.UTC)
    seaqa_db.update_connection(connection_id, indexed_at, json.dumps(connection_status))


def update_seafile_index(project_connection, seasearch_api, seaqa_db):
    connection_id = project_connection.id
    project_uuid = project_connection.project_uuid
    config = decrypt_config(json.loads(project_connection.config))
    repo_id = config.get('repo_id')
    old_indexed_at = project_connection.indexed_at
    connection_type = project_connection.type
    connection_status = json.loads(project_connection.status)

    seafile_seadb_api = SeafileSeaDBAPI(project_uuid)

    seafile_index = SeafileIndex(seasearch_api)

    since = get_since_time(old_indexed_at)

    seafile_index_name = gen_index_name_by_type(connection_id, connection_type)
    seafile_index.create_index_if_missing(seafile_index_name)
    index_count = seafile_index.update(seafile_index_name, connection_id, repo_id, connection_status, seaqa_db, seafile_seadb_api, since)

    # finish index
    connection_status['last_indexed_count'] = index_count
    connection_status['last_index_status'] = IndexStatus.COMPLETED
    new_indexed_at = datetime.datetime.now(datetime.UTC)
    seaqa_db.update_connection(connection_id, new_indexed_at, json.dumps(connection_status))


def update_github_issues_index(project_connection, seasearch_api, seaqa_db):
    connection_id = project_connection.id
    config = decrypt_config(json.loads(project_connection.config))
    connection_type = project_connection.type
    server_url = config.get('repository')
    path = urlparse(server_url).path
    parts = path.strip("/").split("/")
    repo_owner, repo_name = parts[0], parts[1]
    connection_status = json.loads(project_connection.status)
    old_indexed_at = project_connection.indexed_at
    project_uuid = project_connection.project_uuid
    github_seadb_api = GitHubSeaDBAPI(project_uuid)
    github_issues_index = GitHubIssuesIndex(seasearch_api)

    connection_status['last_index_status'] = IndexStatus.INDEXING
    seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))

    issues_index_name = gen_index_name_by_type(connection_id, connection_type)
    github_issues_index.create_index_if_missing(issues_index_name)

    since = get_since_time(old_indexed_at)

    index_count = github_issues_index.update(issues_index_name, connection_id, repo_owner, repo_name, connection_status, seaqa_db, github_seadb_api, since)
    # finish index
    indexed_at = datetime.datetime.now(datetime.UTC)
    connection_status['last_indexed_count'] = index_count
    connection_status['last_index_status'] = IndexStatus.COMPLETED
    seaqa_db.update_connection(connection_id, indexed_at, json.dumps(connection_status))


def update_discourse_forum_index(project_connection, seasearch_api, seaqa_db):
    connection_id = project_connection.id
    connection_type = project_connection.type
    project_uuid = project_connection.project_uuid
    old_indexed_at = project_connection.indexed_at
    connection_status = json.loads(project_connection.status)

    discourse_seadb_api = DiscourseSeaDBAPI(project_uuid)
    discourse_forum_index = DiscourseIndex(seasearch_api)

    connection_status['last_index_status'] = IndexStatus.INDEXING
    seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
    discourse_index_name = gen_index_name_by_type(connection_id, connection_type)
    discourse_forum_index.create_index_if_missing(discourse_index_name)

    since = get_since_time(old_indexed_at)

    index_count = discourse_forum_index.update(discourse_index_name, connection_id, connection_status, seaqa_db, discourse_seadb_api, since)
    indexed_at = datetime.datetime.now(datetime.UTC)
    connection_status['last_indexed_count'] = index_count
    connection_status['last_index_status'] = IndexStatus.COMPLETED
    seaqa_db.update_connection(connection_id, indexed_at, json.dumps(connection_status))


def update_email_index(project_connection, seasearch_api, seaqa_db):
    connection_id = project_connection.id
    connection_type = project_connection.type
    project_uuid = project_connection.project_uuid
    old_indexed_at = project_connection.indexed_at
    connection_status = json.loads(project_connection.status)

    email_seadb_api = EmailSeaDBAPI(project_uuid)
    email_index = EmailIndex(seasearch_api)

    connection_status['last_index_status'] = IndexStatus.INDEXING
    seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
    index_name = gen_index_name_by_type(connection_id, connection_type)
    email_index.create_index_if_missing(index_name)

    since = get_since_time(old_indexed_at)

    index_count = email_index.update(index_name, connection_id, connection_status, seaqa_db, email_seadb_api, since)
    indexed_at = datetime.datetime.now(datetime.UTC)
    connection_status['last_indexed_count'] = index_count
    connection_status['last_index_status'] = IndexStatus.COMPLETED
    seaqa_db.update_connection(connection_id, indexed_at, json.dumps(connection_status))


def update_knowledge_base_index(project_uuid, db_session_class):
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
        since = get_since_time(old_indexed_at)

        indexed_count = kb_index.update(index_name, uuid_str_to_32_chars(project_uuid), kb_seadb_api, since)
        indexed_at = datetime.datetime.now(datetime.UTC)
        seaqa_db.update_knowledge_base_indexed_at(project_uuid, indexed_at)
        logger.info('Finish to index knowledge base project_uuid %s', project_uuid)
    except Exception as e:
        logger.exception('update knowledge base: %s index failed, error: %s', project_uuid, e)


def update_ticket_index(project_uuid, db_session_class):
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
        since = get_since_time(old_indexed_at)

        indexed_count = ticket_index.update(index_name, uuid_str_to_32_chars(project_uuid), ticket_seadb_api, since)
        indexed_at = datetime.datetime.now(datetime.UTC)
        seaqa_db.update_ticket_indexed_at(project_uuid, indexed_at)
        logger.info('Finish to index ticket project_uuid %s', project_uuid)
    except Exception as e:
        logger.exception('update ticket: %s index failed, error: %s', project_uuid, e)


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

    content_vector_status['last_content_vector_index_status'] = IndexStatus.INDEXING
    seaqa_db.update_content_vector_status_and_time(connection_id, json.dumps(content_vector_status))

    project_uuid = connection.project_uuid
    last_content_vector_indexed_at = connection.content_vector_indexed_at

    try:
        # Only support SITE type for now
        if connection_type != ConnectionType.SITE.value:
            logger.warning('Unsupported connection type: %s for connection %s', connection_type, connection_id)
            return

        handler = get_handler(connection_type)
        if not handler:
            logger.warning('Unsupported connection type: %s for connection %s', connection_type, connection_id)
            return
        table_name = handler.gen_table_name(connection_id)
        content_vector_index = ContentVectorIndex(seasearch_api)
        content_index_name = gen_content_vector_index_name(connection_id, connection_type)
        content_vector_index.create_index_if_missing(content_index_name)

        since = get_since_time(last_content_vector_indexed_at)

        index_count = content_vector_index.update(
            table_name,
            content_index_name,
            connection_id,
            project_uuid,
            content_vector_status,
            seaqa_db,
            seadb_api,
            storage,
            since
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
        handler = get_handler(connection_type)
        if not handler:
            logger.warning('Unsupported connection type: %s for connection %s', connection_type, connection_id)
            ai_status['last_ai_index_status'] = IndexStatus.FAILED
            seaqa_db.update_ai_status_and_time(connection_id, json.dumps(ai_status))
            return
        table_name = handler.gen_table_name(connection_id)

        fixed_count = fix_summary(project_uuid, table_name, seadb_api)
        if fixed_count > 0:
            logger.info('Fixed %d missing embeddings for connection %s before indexing', fixed_count, connection_id)

        summary_vector_index = SummaryVectorIndex(seasearch_api)
        ai_index_name = gen_conn_summary_index_name(connection_id, connection_type)
        summary_vector_index.create_index_if_missing(ai_index_name)

        since = get_since_time(last_ai_indexed_at)

        index_count = summary_vector_index.update(
            table_name,
            ai_index_name,
            connection_id,
            project_uuid,
            ai_status,
            seaqa_db,
            seadb_api,
            since,
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
    fixed_count = fix_summary(project_uuid, table_name, seadb_api)
    if fixed_count > 0:
        logger.info('Fixed %d missing embeddings for knowledge base of project: %s before indexing', fixed_count, project_uuid)

    kb_summary_vector_index = KnowledgeBaseSummaryVectorIndex(seasearch_api)
    index_name = gen_kb_summary_index_name(project_uuid)

    kb_summary_vector_index.create_index_if_missing(index_name)

    since = get_since_time(last_kb_ai_indexed_at)
    try:
        indexed_count = kb_summary_vector_index.update(table_name, index_name, project_uuid, seadb_api, since)
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

    fixed_count = fix_summary(project_uuid, table_name, seadb_api)
    if fixed_count > 0:
        logger.info('Fixed %d missing embeddings for tickets of project: %s before indexing', fixed_count, project_uuid)

    ticket_summary_vector_index = TicketSummaryVectorIndex(seasearch_api)
    ai_index_name = gen_ticket_summary_index_name(project_uuid)
    ticket_summary_vector_index.create_index_if_missing(ai_index_name)

    since = get_since_time(last_ticket_ai_indexed_at)

    index_count = ticket_summary_vector_index.update(table_name, ai_index_name, project_uuid, seadb_api, since)
    return index_count
