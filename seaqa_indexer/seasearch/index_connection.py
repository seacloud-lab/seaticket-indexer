import logging
import json
import datetime
from urllib.parse import urlparse

from seaqa_indexer.seadb_models.github_seadb_api import GitHubSeaDBAPI
from seaqa_indexer.utils.constants import ConnectionType, IndexStatus
from seaqa_indexer.config import SEASEARCH_URL, SEASEARCH_TOKEN
from seaqa_indexer.seasearch.index_store.site_index import SiteIndex
from seaqa_indexer.seasearch.index_store.seafile_index import SeafileIndex
from seaqa_indexer.seasearch.index_store.github_issues_index import GitHubIssuesIndex
from seaqa_indexer.seasearch.index_store.discourse_index import DiscourseIndex
from seaqa_indexer.seasearch.index_store.email_index import EmailIndex
from seaqa_indexer.seasearch.index_store.knowledge_base_index import KnowledgeBaseIndex
from seaqa_indexer.utils.seasearch_api import SeaSearchAPI
from seaqa_indexer.seasearch.utils import gen_index_name_by_type, gen_kb_index_name
from seaqa_indexer.utils import time_to_utc_time, decrypt_config
from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.seadb_models.discourse_seadb_api import DiscourseSeaDBAPI
from seaqa_indexer.seadb_models.seafile_seadb_api import SeafileSeaDBAPI
from seaqa_indexer.seadb_models.email_seadb_api import EmailSeaDBAPI
from seaqa_indexer.seadb_models.knowledge_base_seadb_api import KnowledgeBaseSeaDBAPI


logger = logging.getLogger(__name__)


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
        if connection_type == ConnectionType.SITE.value:
            update_site_index(connection, seasearch_api, seaqa_db)
        elif connection_type == ConnectionType.SEAFILE.value:
            update_seafile_index(connection, seasearch_api, seaqa_db)
        elif connection_type == ConnectionType.GITHUB_ISSUE.value:
            update_github_issues_index(connection, seasearch_api, seaqa_db)
        elif connection_type == ConnectionType.DISCOURSE_FORUM.value:
            update_discourse_forum_index(connection, seasearch_api, seaqa_db)
        elif connection_type == ConnectionType.EMAIL.value:
            update_email_index(connection, seasearch_api, seaqa_db)
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

    since = old_indexed_at
    if not since:
        since = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    since = time_to_utc_time(str(since)).isoformat()

    index_count = site_index.update(site_index_name, connection_id, project_uuid, connection_status, seaqa_db, since)
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

    since = old_indexed_at
    if not since:
        since = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    since = time_to_utc_time(str(since)).isoformat()

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

    since = old_indexed_at
    if not since:
        since = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    since = time_to_utc_time(str(since)).isoformat()

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

    since = old_indexed_at
    if not since:
        since = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    since = time_to_utc_time(str(since)).isoformat()

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

    since = old_indexed_at
    if not since:
        since = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    since = time_to_utc_time(str(since)).isoformat()

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