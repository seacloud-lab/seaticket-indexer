import logging
import json
import datetime
import time
import os

from seaqa_indexer.utils.constants import CrawlStatus, REPO_NAME_CACHE_TIME_OUT, REPO_FILE_SIZE_LIMIT, SYNC_FILE_SUFFIXES
from seaqa_indexer.utils import decrypt_config, time_to_utc_time, encrypt_config, get_file_suffix, \
    need_update_connection_last_sync_count
from seaqa_indexer.utils.github_issues_api import GitHubAPI
from seaqa_indexer.index.utils import get_content_by_download_link
from seaqa_indexer.utils.seahub_api import FileNotFoundException
from seaqa_indexer.index.extract import ExtractorFactory
from seaqa_indexer.app.event_redis import redis_cache
from seaqa_indexer.utils.sync_logger import SyncLogger
from seaqa_indexer.syncer.utils import upload_email_attachment, insert_and_update_thread_rows, EMAIL_SYNC_INTERVAL_DAYS
from seaqa_indexer.handlers import get_handler_or_none

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

    try:
        conn_handler = get_handler_or_none(connection_type)
        if not conn_handler:
            logger.warning('Unsupported connection type: %s for connection %s', connection_type, connection_id)
            sync_logger.log(f'Unsupported connection type: {connection_type}')
            connection_status['last_sync_status'] = CrawlStatus.FAILED
            seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
            sync_logger.stop_capture()
            return
        
        sync_result = conn_handler.sync(
            connection_id, project_uuid, config, old_last_sync_time,
            connection_status, seaqa_db, sync_logger
        )
        
        if sync_result is None or (isinstance(sync_result, tuple) and None in sync_result[:2]):

            connection_status['last_sync_status'] = CrawlStatus.FAILED
            seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
            sync_logger.stop_capture()
            return
        
        sync_count, total_count, is_update_config, config = sync_result
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

    logger.info('Finish sync connection: %s, last sync count %s', connection_id, sync_count)
    sync_logger.log(f'Finish sync connection: {connection_id}')
    sync_logger.stop_capture()

    msg_content = {
        'connection_id': connection_id,
        'type': 'connection'
    }
    mq.lpush('index_task', json.dumps(msg_content))
    mq.lpush('ai_process_task', json.dumps(msg_content))


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
            existed_issue = github_seadb_api.get_issue_by_issue_id(issue_id, connection_id)
            if existed_issue:
                if time_to_utc_time(issue.get('updated_at')) <= time_to_utc_time(str(existed_issue.get('modified_time'))):
                    logger.debug(f'Issue {issue_id} is up to date, skipping sync.')
                    continue

            sync_count += 1
            github_seadb_api.save_or_update_issue_to_db(connection_id, issue, existed_issue)
            should, new_time, new_count = need_update_connection_last_sync_count(
                sync_count, _status_last_update_time, _status_last_update_count
            )
            if should:
                connection_status['last_sync_count'] = sync_count
                seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
                _status_last_update_time = new_time
                _status_last_update_count = new_count

            # sync issue comments
            comments_page = 1
            all_comments = []
            while True:
                comments = github_api.get_issue_comments(issue['number'], per_page=per_page, page=comments_page)
                all_comments += comments
                comments_page += 1
                if not comments or len(comments) < per_page:
                    break

            github_seadb_api.save_or_update_issue_comments(connection_id, issue_id, all_comments)

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
            modified_time = file.get('_file_mtime')
            size = file.get('_size')

            file_suffix = get_file_suffix(dirent_name)
            if file_suffix not in SYNC_FILE_SUFFIXES:
                continue

            existed_file = seafile_seadb_api.get_file_by_path(connection_id, parent_dir, dirent_name)
            if existed_file:
                if time_to_utc_time(modified_time) <= time_to_utc_time(str(existed_file.get('modified_time'))):
                    logger.debug(f'connection_id %s parent_dir %s filename: %s is up to date, skipping sync.', connection_id, parent_dir, dirent_name)
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
                'modified_time': modified_time,
                'size': size,
                'content': content if content else '',
            }
            seafile_seadb_api.save_or_update_file_to_db(connection_id, file_info, existed_file)
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
            path = '/' + file.get('path').strip('/')
            file_info = {
                'path': path,
                'filename': file.get('obj_name'),
            }
            seafile_seadb_api.mark_file_as_deleted(connection_id, file_info)

    total_count = seafile_seadb_api.get_file_total_count(connection_id)
    return sync_count, total_count


def sync_email(connection_id, project_uuid, connection_status, since, imap, seaqa_db, email_seadb_api, storage, is_first_sync):
    sync_count = 0
    _status_last_update_time = datetime.datetime.now()
    _status_last_update_count = 0

    since = time_to_utc_time(str(since))
    today = datetime.datetime.now(datetime.timezone.utc).date()

    # Due to time zone differences, emails sent on the same day may not be available for retrieval within that time frame.
    # Therefore, the search scope has been expanded to include a full day.
    before_date = since.date() - datetime.timedelta(days=1)

    while before_date <= today:
        since_date = before_date
        before_date += datetime.timedelta(days=EMAIL_SYNC_INTERVAL_DAYS)

        email_list = imap.search_emails(since_date, before_date)
        new_email_list = []
        for email in email_list:
            if not email:
                continue
            existed_email = {}
            if not is_first_sync:
                message_id = email.get('message_id')
                modified_time = email.get('email_date')
                existed_email = email_seadb_api.get_email_by_message_id(connection_id, message_id)
                if existed_email:
                    existed_date = time_to_utc_time(str(existed_email.get('modified_time')))
                    email_thread_id = existed_email.get('thread_id')
                    if time_to_utc_time(modified_time) <= existed_date and email_thread_id:
                        logger.debug(f'connection_id %s message_id %s is up to date, skipping sync.', connection_id, message_id)
                        continue
            email_info = {
                'message_id': email.get('message_id'),
                'reply_to_message_id': email.get('reply_to_message_id'),
                'email_date': email.get('email_date'),
                'subject': email.get('subject'),
                'is_sender': email.get('is_sender'),
            }
            new_email_list.append(email_info)

            upload_email_attachment(connection_id, project_uuid, storage, email)

            email_seadb_api.save_or_update_email_to_db(connection_id, email, existed_email)
            should, new_time, new_count = need_update_connection_last_sync_count(
                sync_count, _status_last_update_time, _status_last_update_count
            )
            if should:
                connection_status['last_sync_count'] = sync_count
                seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
                _status_last_update_time = new_time
                _status_last_update_count = new_count
            sync_count += 1
        if new_email_list:
            insert_and_update_thread_rows(email_seadb_api, connection_id, since, new_email_list, is_first_sync)

    total_count = email_seadb_api.get_email_total_count(connection_id)
    return sync_count, total_count


def sync_discourse(connection_id, sync_earliest_time, last_sync_time, connection_status, discourse_forum_api, discourse_seadb_api, seaqa_db):
    page = 0
    should_stop = False
    sync_count = 0
    _status_last_update_time = datetime.datetime.now()
    _status_last_update_count = 0

    if last_sync_time:
        last_sync_time = time_to_utc_time(str(last_sync_time))

    while True:
        topics = discourse_forum_api.fetch_latest_topics_page(page)

        if not topics:
            break

        for topic_data in topics:
            topic_modified_time = time_to_utc_time(topic_data.get('bumped_at'))

            if last_sync_time and topic_modified_time <= last_sync_time and not topic_data.get('pinned'):
                should_stop = True
                break

            if topic_modified_time <= sync_earliest_time:
                if topic_data.get('pinned'):
                    continue
                should_stop = True
                break

            topic_id = topic_data.get('id')

            # Check if topic already exists in DB
            existing_topic = discourse_seadb_api.get_topic_by_topic_id(connection_id, topic_id)
            if existing_topic:
                existing_modified_time = time_to_utc_time(str(existing_topic.get('modified_time')))
                if topic_modified_time <= existing_modified_time:
                    logger.debug(f'Topic {topic_id} is up to date, skipping sync.')
                    continue

            primary_key = existing_topic.get('_pk')
            if primary_key:
                discourse_seadb_api.update_topic(connection_id, primary_key, topic_data)
            else:
                discourse_seadb_api.insert_topic(connection_id, topic_id, topic_data)

            # sync topic replies
            topic_replies = discourse_forum_api.fetch_topic_replies(topic_id)
            discourse_seadb_api.update_topic_replies(connection_id, topic_id, topic_replies)

            sync_count += 1
            should, new_time, new_count = need_update_connection_last_sync_count(
                sync_count, _status_last_update_time, _status_last_update_count
            )
            if should:
                connection_status['last_sync_count'] = sync_count
                seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
                _status_last_update_time = new_time
                _status_last_update_count = new_count
            time.sleep(1)

        page += 1
        if should_stop:
            break

    total_count = discourse_seadb_api.get_topic_count(connection_id)
    return sync_count, total_count
