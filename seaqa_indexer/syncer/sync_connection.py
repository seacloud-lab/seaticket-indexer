import logging
import json
import datetime

from urllib.parse import urlparse

from seaqa_indexer.seadb_models.github_seadb_api import GitHubSeaDBAPI
from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.syncer.discourse_syncer.discourse_sync import DiscourseSync
from seaqa_indexer.utils.constants import CrawlStatus, ConnectionType
from seaqa_indexer.utils import decrypt_config
from seaqa_indexer.utils import need_update_connection_status
from seaqa_indexer.utils.github_issues_api import GitHubAPI


logger = logging.getLogger(__name__)


def sync_connection(connection_id, seaqa_db, mq):
    logger.info(f"Start sync connection: {connection_id}")
    project_connection = seaqa_db.get_project_connection_by_id(connection_id)
    if not project_connection:
        logger.warning('connection: %s not found', connection_id)
        return

    connection_is_active = project_connection.is_active
    if not connection_is_active:
        logger.warning('connection: %s is inactive', connection_id)
        return

    connection_status = json.loads(project_connection.status)

    connection_id = project_connection.id
    project_uuid = project_connection.project_uuid
    connection_type = project_connection.type
    old_last_sync_time = project_connection.last_sync_time
    connection_status['last_sync_status'] = CrawlStatus.CRAWLING
    seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))

    config = json.loads(project_connection.config)
    config = decrypt_config(config)

    sync_count = 0
    total_count = 0
    try:
        # sync discourse
        if connection_type == ConnectionType.DISCOURSE_FORUM.value:
            forum_url = config['url']
            discourse_api_key = config['api_key']
            discourse_api_username = config['api_username']
            sync_years = config.get('sync_years', 5)

            project_uuuid = project_connection.project_uuid
            project_created_at = project_connection.created_at
            discourse_sync = DiscourseSync(project_uuuid, seaqa_db)
            sync_count = discourse_sync.sync(
                connection_id,
                forum_url,
                sync_years,
                project_created_at,
                discourse_api_key,
                discourse_api_username,
                old_last_sync_time,
                connection_status,
            )

            total_count = discourse_sync.get_discourse_forum_count(connection_id)

        if connection_type == ConnectionType.GITHUB_ISSUE.value:
            server_url = config.get('repository')
            token = config.get('access_token')
            try:
                path = urlparse(server_url).path
                parts = path.strip("/").split("/")
                repo_owner, repo_name = parts[0], parts[1]
            except Exception as e:
                logger.warning("github repository is invalid: %s", e)
                connection_status['last_sync_status'] = CrawlStatus.FAILED
                seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
                return
            since = old_last_sync_time.isoformat() + "Z" if old_last_sync_time else None
            github_api = GitHubAPI(token, repo_owner, repo_name, connection_id)
            github_seadb_api = GitHubSeaDBAPI(project_uuid)
            sync_count, total_count = sync_github_issue(connection_id, since, connection_status, github_api, seaqa_db, github_seadb_api)
    except Exception as e:
        logger.exception('connection: %s, sync issues failed, error: %s', connection_id, e)
        connection_status['last_sync_status'] = CrawlStatus.FAILED
        seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
        return

    # finish sync
    connection_status['total_records'] = total_count
    connection_status['last_sync_count'] = sync_count
    connection_status['last_sync_status'] = CrawlStatus.COMPLETED
    last_sync_time = datetime.datetime.now(datetime.UTC)
    seaqa_db.update_status_and_time(connection_id, json.dumps(connection_status), last_sync_time)

    logger.info('Finish sync connection: %s', connection_id)
    msg_content = {
        'connection_id': connection_id,
    }
    mq.lpush('index_task', json.dumps(msg_content))


def sync_github_issue(connection_id, since, connection_status, github_api, seaqa_db, github_seadb_api):
    page = 1
    per_page = 100  # max 100
    sync_count = 0

    issues_labels = github_api.get_issues_labels(page, per_page)
    github_seadb_api.add_or_update_issue_label_column_options(connection_id, issues_labels)

    while True:
        issues = github_api.list_issues(since=since, state='all', per_page=per_page, page=page)
        if not issues:
            break

        for issue in issues:
            if 'pull_request' in issue:
                continue
            sync_count += 1
            github_seadb_api.save_or_update_issue_to_db(connection_id, issue)
            if need_update_connection_status(sync_count):
                connection_status['last_sync_count'] = sync_count
                seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
            comments_page = 1
            while True:
                comments = github_api.get_issue_comments(issue['number'], per_page=per_page, page=comments_page)
                comments_page += 1
                if not comments:
                    break
                for comment in comments:
                    github_seadb_api.save_or_update_issue_comment_to_db(connection_id, issue['id'], comment)

        if len(issues) < per_page:
            break
        page += 1

    total_count = github_seadb_api.get_github_issues_count(connection_id)
    return sync_count, total_count
