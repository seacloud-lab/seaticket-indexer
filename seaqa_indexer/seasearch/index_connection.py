import logging
import json
import datetime
from urllib.parse import urlparse

from seaqa_indexer.utils.constants import ConnectionType, REPO_NAME_CACHE_TIME_OUT, IndexStatus
from seaqa_indexer.config import SEASEARCH_URL, SEASEARCH_TOKEN
from seaqa_indexer.seasearch.index_store.site_index import SiteIndex
from seaqa_indexer.seasearch.index_store.seafile_index import SeafileIndex
from seaqa_indexer.seasearch.index_store.github_issues_index import GitHubIssuesIndex
from seaqa_indexer.seasearch.index_store.discourse_index import DiscourseIndex
from seaqa_indexer.utils.seasearch_api import SeaSearchAPI
from seaqa_indexer.seasearch.utils import gen_index_name_by_type
from seaqa_indexer.utils.seahub_api import SeahubAPI
from seaqa_indexer.utils import time_str_to_utc_time, decrypt_config, encrypt_config
from seaqa_indexer.app.event_redis import redis_cache
from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.seadb_models.discourse_seadb_api import DiscourseSeaDBAPI


logger = logging.getLogger(__name__)


def update_connection_index(connection_id, db_session_class):
    logger.info('Start to index project connection %s', connection_id)
    seaqa_db = SeaqaDB(db_session_class)
    connection = seaqa_db.get_project_connection_by_id(connection_id)
    if not connection:
        logger.warning('Project connection %s does not exist')
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
            update_github_issues_index(connection, seasearch_api, db_session_class, seaqa_db)
        elif connection_type == ConnectionType.DISCOURSE_FORUM.value:
            update_discourse_forum_index(connection, seasearch_api, seaqa_db)
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

    index_count = site_index.update(site_index_name, connection_id, project_uuid, connection_status, seaqa_db, old_indexed_at)
    connection_status['last_indexed_count'] = index_count
    connection_status['last_index_status'] = IndexStatus.COMPLETED
    indexed_at = datetime.datetime.now(datetime.UTC)
    seaqa_db.update_connection(connection_id, indexed_at, json.dumps(connection_status))


def update_seafile_index(project_connection, seasearch_api, seaqa_db):
    connection_id = project_connection.id
    config = decrypt_config(json.loads(project_connection.config))
    server_url = config.get('server_url')
    api_token = config.get('api_token')
    old_repo_id = config.get('repo_id')
    old_last_modified = project_connection.indexed_at
    connection_type = project_connection.type
    connection_status = json.loads(project_connection.status)

    seafile_index = SeafileIndex(seasearch_api)
    seahub_api = SeahubAPI(server_url, api_token)
    repo_info = seahub_api.get_repo_info()
    new_repo_id = repo_info.get('repo_id')
    new_last_modified = repo_info.get('last_modified')
    new_repo_name = repo_info.get('repo_name')

    redis_cache.set(new_repo_id, json.dumps({server_url: new_repo_name}), REPO_NAME_CACHE_TIME_OUT)

    new_last_modified = time_str_to_utc_time(new_last_modified)
    new_last_sync_time = datetime.datetime.now(datetime.UTC)
    if new_repo_id == old_repo_id and str(old_last_modified) == str(new_last_modified).split('+')[0]:
        connection_status['last_indexed_count'] = 0
        connection_status['last_index_status'] = IndexStatus.COMPLETED
        seaqa_db.update_connection(connection_id, old_last_modified, json.dumps(connection_status), last_sync_time=new_last_sync_time)
        logger.info('connection: %s, repo: %s does not change, skip this update', connection_id, new_repo_id)
        return

    seafile_index_name = gen_index_name_by_type(connection_id, connection_type)
    seafile_index.create_index_if_missing(seafile_index_name)
    index_count, total_records = seafile_index.update(seafile_index_name, connection_id, new_repo_id, seahub_api, connection_status, seaqa_db)

    # finish index
    config['repo_id'] = new_repo_id
    config['repo_name'] = new_repo_name
    connection_status['last_indexed_count'] = index_count
    connection_status['last_index_status'] = IndexStatus.COMPLETED
    connection_status['total_records'] = total_records
    new_indexed_at = datetime.datetime.now(datetime.UTC)
    seaqa_db.update_connection(connection_id, new_indexed_at, json.dumps(connection_status), last_sync_time=new_last_sync_time, config=encrypt_config(config))


def update_github_issues_index(project_connection, seasearch_api, db_session_class, seaqa_db):
    connection_id = project_connection.id
    config = decrypt_config(json.loads(project_connection.config))
    connection_type = project_connection.type
    server_url = config.get('repository')
    path = urlparse(server_url).path
    parts = path.strip("/").split("/")
    repo_owner, repo_name = parts[0], parts[1]
    connection_status = json.loads(project_connection.status)
    old_indexed_at = project_connection.indexed_at
    github_issues_index = GitHubIssuesIndex(seasearch_api, db_session_class)

    connection_status['last_index_status'] = IndexStatus.INDEXING
    seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))

    issues_index_name = gen_index_name_by_type(connection_id, connection_type)
    github_issues_index.create_index_if_missing(issues_index_name)

    index_count = github_issues_index.update(issues_index_name, connection_id, project_uuid, repo_owner, repo_name, connection_status, seaqa_db, old_indexed_at)
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
    discourse_forum_index = DiscourseIndex(seasearch_api, discourse_seadb_api)

    connection_status['last_index_status'] = IndexStatus.INDEXING
    seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
    discourse_index_name = gen_index_name_by_type(connection_id, connection_type)
    discourse_forum_index.create_index_if_missing(discourse_index_name)

    index_count = discourse_forum_index.update(discourse_index_name, connection_id, connection_status, seaqa_db, old_indexed_at)
    indexed_at = datetime.datetime.now(datetime.UTC)
    connection_status['last_indexed_count'] = index_count
    connection_status['last_index_status'] = IndexStatus.COMPLETED
    seaqa_db.update_connection(connection_id, indexed_at, json.dumps(connection_status))
