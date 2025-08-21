import logging
import json
import datetime

from urllib.parse import urlparse

from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.syncer.discourse_syncer.discourse_sync import DiscourseSync
from seaqa_indexer.syncer.github_issues_syncer.github_issues_sync import GithubIssuesSync
from seaqa_indexer.utils.constants import CrawlStatus, ConnectionType
from seaqa_indexer.utils import decrypt_config


logger = logging.getLogger(__name__)


def sync_connection(connection_id, db_session_class, mq):
    logger.info(f"Start sync connection: {connection_id}")
    seaqa_db = SeaqaDB(db_session_class)
    project_connection = seaqa_db.get_project_connection_by_id(connection_id)
    if not project_connection:
        logger.warning('connection: %s not found', connection_id)
        return

    connection_is_active = project_connection.is_active
    if not connection_is_active:
        logger.warning('connection: %s is inactive', connection_id)
        return

    connection_status = json.loads(project_connection.status)
    last_sync_status = connection_status.get('last_sync_status')
    if last_sync_status == CrawlStatus.CRAWLING:
        logger.warning('connection: %s is syncing', connection_id)
        return

    connection_id = project_connection.id
    connection_type = project_connection.type
    last_updated_at = project_connection.updated_at
    connection_status['last_sync_status'] = CrawlStatus.CRAWLING
    seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))

    config = json.loads(project_connection.config)
    config = decrypt_config(config)

    sync_count = 0
    total_count = 0
    new_updated_at = None
    try:
        # sync discourse
        if connection_type == ConnectionType.DISCOURSE_FORUM.value:
            forum_url = config['url']
            discourse_api_key = config['api_key']
            discourse_api_username = config['api_username']

            discourse_sync = DiscourseSync(db_session_class)
            latest_bumped_at, sync_count = discourse_sync.sync(connection_id, forum_url, discourse_api_key, discourse_api_username, last_updated_at, connection_status)

            new_updated_at = latest_bumped_at if latest_bumped_at else last_updated_at
            total_count = discourse_sync.get_discourse_forum_count(connection_id)

        if connection_type == ConnectionType.GITHUB_ISSUE.value:
            server_url = config.get('repository')
            token = config.get('access_token')
            old_updated_at = project_connection.updated_at
            try:
                path = urlparse(server_url).path
                parts = path.strip("/").split("/")
                repo_owner, repo_name = parts[0], parts[1]
            except Exception as e:
                logger.warning("github repository is invalid: %s", e)
                return
            since = old_updated_at.isoformat() + "Z" if old_updated_at else None
            new_updated_at = datetime.datetime.now(datetime.UTC)
            github_issues_sync = GithubIssuesSync(db_session_class)
            sync_count = github_issues_sync.sync(connection_id, token, repo_owner, repo_name, since, connection_status)
            total_count = github_issues_sync.get_github_issues_count(connection_id)
    except Exception as e:
        logger.exception('connection: %s, sync issues failed, error: %s', connection_id, e)
        connection_status['last_sync_status'] = CrawlStatus.FAILED
        seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
        return

    # finish sync
    if not new_updated_at:
        new_updated_at = last_updated_at
    connection_status['total_records'] = total_count
    connection_status['last_sync_count'] = sync_count
    connection_status['last_sync_status'] = CrawlStatus.COMPLETED
    seaqa_db.update_status_and_time(connection_id, json.dumps(connection_status), new_updated_at)

    logger.info('Finish sync connection: %s', connection_id)
    msg_content = {
        'connection_id': connection_id,
    }
    mq.lpush('index_task', json.dumps(msg_content))
