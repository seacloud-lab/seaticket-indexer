import logging
import json
import datetime
import os

from urllib.parse import urlparse

from seaqa_indexer.seadb_models.github_seadb_api import GitHubSeaDBAPI
from seaqa_indexer.syncer.discourse_syncer.discourse_sync import DiscourseSync
from seaqa_indexer.utils.constants import CrawlStatus, ConnectionType, REPO_NAME_CACHE_TIME_OUT
from seaqa_indexer.utils import decrypt_config, time_str_to_utc_time, encrypt_config, get_file_suffix
from seaqa_indexer.utils import need_update_connection_last_sync_count
from seaqa_indexer.utils.github_issues_api import GitHubAPI
from seaqa_indexer.seasearch.utils import get_content_by_download_link
from seaqa_indexer.utils.seahub_api import FileNotFoundException
from seaqa_indexer.seasearch.extract import ExtractorFactory
from seaqa_indexer.utils.constants import REPO_FILE_SIZE_LIMIT, SEADB_CONTENT_SIZE_LIMIT, SYNC_FILE_SUFFIXES
from seaqa_indexer.utils.seahub_api import SeahubAPI
from seaqa_indexer.seadb_models.seafile_seadb_api import SeafileSeaDBAPI
from seaqa_indexer.app.event_redis import redis_cache
from seaqa_indexer.utils.sync_logger import SyncLogger

logger = logging.getLogger(__name__)

def sync_connection(connection_id, seaqa_db, mq, db_session_class):
    logger.info(f"Start sync connection: {connection_id}")
    sync_logger = SyncLogger(db_session_class).init(connection_id)
    sync_logger.log(f"Start sync connection: {connection_id}")
    project_connection = seaqa_db.get_project_connection_by_id(connection_id)
    if not project_connection:
        logger.warning('connection: %s not found', connection_id)
        sync_logger.log(f'connection: {connection_id} not found')
        sync_logger.stop_capture()
        return

    connection_is_active = project_connection.is_active
    if not connection_is_active:
        logger.warning('connection: %s is inactive', connection_id)
        sync_logger.log(f'connection: {connection_id} is inactive')
        sync_logger.stop_capture()
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
    is_update_config = False

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
                sync_logger,
            )

            total_count = discourse_sync.get_discourse_forum_topic_count(connection_id)

        if connection_type == ConnectionType.GITHUB_ISSUE.value:
            server_url = config.get('repository')
            token = config.get('access_token')
            installation_id = config.get('installation_id')
            try:
                path = urlparse(server_url).path
                parts = path.strip("/").split("/")
                repo_owner, repo_name = parts[0], parts[1]
            except Exception as e:
                logger.warning("github repository is invalid: %s", e)
                sync_logger.log(f"Github repository is invalid")
                connection_status['last_sync_status'] = CrawlStatus.FAILED
                seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
                sync_logger.stop_capture()
                return
            since = old_last_sync_time.isoformat() + "Z" if old_last_sync_time else None
            github_api = GitHubAPI(token, repo_owner, repo_name, connection_id)
            github_seadb_api = GitHubSeaDBAPI(project_uuid)
            sync_count, total_count = sync_github_issue(connection_id, since, connection_status, github_api, seaqa_db, github_seadb_api)

        if connection_type == ConnectionType.SEAFILE.value:
            old_repo_name = config.get('repo_name')
            server_url = config.get('server_url')
            api_token = config.get('api_token')
            since = old_last_sync_time
            is_first_update = False
            if not since:
                since = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
                is_first_update = True
            since = time_str_to_utc_time(str(since)).isoformat()
            seahub_api = SeahubAPI(server_url, api_token)
            repo_info = seahub_api.get_repo_info()
            repo_id = repo_info.get('repo_id')
            new_repo_name = repo_info.get('repo_name')

            redis_cache.set(repo_id, json.dumps({server_url: new_repo_name}), REPO_NAME_CACHE_TIME_OUT)
            config['repo_id'] = repo_id
            config['repo_name'] = new_repo_name
            if old_repo_name != new_repo_name:
                is_update_config = True

            seafile_seadb_api = SeafileSeaDBAPI(project_uuid)
            sync_count, total_count = sync_seafile(connection_id, repo_id, connection_status, since, seahub_api, seaqa_db, seafile_seadb_api, is_first_update, sync_logger)
    except Exception as e:
        logger.exception('connection: %s, sync failed, error: %s', connection_id, e)
        try:
            if len(e.args) > 0:
                status_raw = e.args[0]
                status_text = str(status_raw)
                if '401' in status_text:
                    sync_logger.log('Sync failed: Unauthorized')
                elif '403' in status_text:
                    sync_logger.log('Sync failed: Forbidden')
                elif '404' in status_text:
                    err_msg = 'Page Not Found'
                    if len(e.args) > 1:
                        detail_raw = e.args[1]
                        try:
                            if isinstance(detail_raw, dict):
                                error = detail_raw
                            elif isinstance(detail_raw, str):
                                error = json.loads(detail_raw)
                            else:
                                error = {}
                            err_msg = error.get('error_msg', err_msg)
                        except Exception:
                            pass
                    sync_logger.log(f'Sync failed: {err_msg}')
                else:
                    sync_logger.log('Internal Server Error')
            else:
                sync_logger.log('Internal Server Error')
        except Exception as e:
            logger.error(e)
            sync_logger.log(f'Internal Server Error')
        connection_status['last_sync_status'] = CrawlStatus.FAILED
        seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
        sync_logger.stop_capture()
        return

    # finish sync
    connection_status['total_records'] = total_count
    connection_status['last_sync_count'] = sync_count
    connection_status['last_sync_status'] = CrawlStatus.COMPLETED
    last_sync_time = datetime.datetime.now(datetime.UTC)
    new_config = None
    if is_update_config:
        new_config = encrypt_config(config)
    seaqa_db.update_status_and_time(connection_id, json.dumps(connection_status), last_sync_time, config=new_config)

    logger.info('Finish sync connection: %s', connection_id)
    sync_logger.log(f'Finish sync connection: {connection_id}')
    sync_logger.stop_capture()
    msg_content = {
        'connection_id': connection_id,
    }
    mq.lpush('index_task', json.dumps(msg_content))


def sync_github_issue(connection_id, since, connection_status, github_api, seaqa_db, github_seadb_api):
    page = 1
    per_page = 100  # max 100
    sync_count = 0
    _status_last_update_time = datetime.datetime.now()
    _status_last_update_count = 0

    issues_labels = github_api.get_issues_labels(page, per_page)
    github_seadb_api.add_or_update_issue_label_column_options(connection_id, issues_labels)

    while True:
        issues = github_api.list_issues(since=since, state='all', per_page=per_page, page=page)
        if not issues:
            break

        for issue in issues:
            if 'pull_request' in issue:
                continue

            # Check if issue already exists in DB
            issue_id = issue['id']
            existing_issue = github_seadb_api.get_issue_by_id(issue_id, connection_id)
            if existing_issue:
                existing_updated_at = time_str_to_utc_time(str(existing_issue.get('updated_at')))
                if time_str_to_utc_time(issue.get('updated_at')) <= existing_updated_at:
                    logger.debug(f'Issue {issue_id} is up to date, skipping sync.')
                    continue

            sync_count += 1
            github_seadb_api.save_or_update_issue_to_db(connection_id, issue)
            should, new_time, new_count = need_update_connection_last_sync_count(
                sync_count, _status_last_update_time, _status_last_update_count
            )
            if should:
                connection_status['last_sync_count'] = sync_count
                seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
                _status_last_update_time = new_time
                _status_last_update_count = new_count
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


def sync_seafile(connection_id, repo_id, connection_status, since, seahub_api, seaqa_db, seafile_seadb_api, is_first_update, sync_logger):
    page = 1
    per_page = 1000
    sync_count = 0
    _status_last_update_time = datetime.datetime.now()
    _status_last_update_count = 0
    # sync updated files
    while True:
        resp = seahub_api.get_recently_changed_files(since, page, per_page)
        page += 1
        recently_changed_files = resp.get('files', [])
        if not recently_changed_files:
            break

        for file in recently_changed_files:
            parent_dir = file.get('_parent_dir')
            dirent_name = file.get('_name')
            mtime = file.get('_file_mtime')
            size = file.get('_size')

            file_suffix = get_file_suffix(dirent_name)
            if file_suffix not in SYNC_FILE_SUFFIXES:
                continue

            existing_file = seafile_seadb_api.get_file_by_path(connection_id, parent_dir, dirent_name)
            if existing_file:
                existing_mtime = time_str_to_utc_time(str(existing_file.get('mtime')))
                if time_str_to_utc_time(mtime) <= existing_mtime:
                    logger.debug(f'connection_id % parent_dir % filename: %s is up to date, skipping sync.', connection_id, parent_dir, dirent_name)
                    continue

            content = ''
            if size < REPO_FILE_SIZE_LIMIT:
                file_path = os.path.join(parent_dir, dirent_name)
                extractor = ExtractorFactory.get_extractor(dirent_name)
                if extractor:
                    try:
                        download_link = seahub_api.get_download_link(os.path.join(parent_dir, dirent_name))
                    except FileNotFoundException:
                        logger.warning('connection: %s, repo_id: %s, parent_dir: %s, name: %s file not found',
                                       connection_id, repo_id, parent_dir, dirent_name)
                        sync_logger.log(f'File not found: {parent_dir}/{dirent_name}')
                        continue
                    content = get_content_by_download_link(download_link)
                    content = extractor.extract(connection_id, repo_id, content, file_path)
            file_info = {
                'path': parent_dir,
                'filename': dirent_name,
                'mtime': mtime,
                'size': size,
                'content': content[:SEADB_CONTENT_SIZE_LIMIT] if content else '',
            }
            seafile_seadb_api.save_or_update_file_to_db(connection_id, file_info)
            should, new_time, new_count = need_update_connection_last_sync_count(
                sync_count, _status_last_update_time, _status_last_update_count
            )
            if should:
                connection_status['last_sync_count'] = sync_count
                seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
                _status_last_update_time = new_time
                _status_last_update_count = new_count
            sync_count += 1

    # sync deleted files
    page = 1
    per_page = 1000
    while True:
        if is_first_update:
            break
        resp = seahub_api.get_recently_deleted_files(since, page, per_page)
        page += 1
        recently_deleted_files = resp.get('files', [])
        if not recently_deleted_files:
            break

        for file in recently_deleted_files:
            file_suffix = get_file_suffix(file.get('obj_name'))
            if file_suffix not in SYNC_FILE_SUFFIXES:
                continue
            file_info = {
                'path': file.get('path'),
                'filename': file.get('obj_name'),
            }
            seafile_seadb_api.mark_file_as_deleted(connection_id, file_info)

    total_count = seafile_seadb_api.get_file_total_count(connection_id)
    return sync_count, total_count
