import datetime
import logging
import json
import hmac
import hashlib

from urllib.parse import urlparse

from sqlalchemy.sql import text

from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
from seaqa_indexer.mq import get_mq
from seaqa_indexer.utils import decrypt_config, need_update_connection_status
from seaqa_indexer.utils.constants import CrawlStatus
from seaqa_indexer.utils.github_issues_api import GitHubAPI

logger = logging.getLogger(__name__)


class SignatureInvalidException(Exception):
    pass


class GithubRepositoryInvalidException(Exception):
    pass


class GithubIssuesSyncManager:
    def __init__(self):
        self.db_session = init_db_session_class()
        self.mq = get_mq(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)

    def save_or_update_issues_to_db(self, connection_id, issue):
        with self.db_session() as session:
            sql = text("""
                INSERT INTO github_issues (
                    issue_id, issue_number, title, body, state,
                    created_at, updated_at, closed_at, labels,
                    comments, connection_id, author, url, need_index
                ) VALUES (
                    :issue_id, :issue_number, :title, :body, :state,
                    :created_at, :updated_at, :closed_at, :labels,
                    :comments, :connection_id, :author, :url, :need_index
                )
                ON DUPLICATE KEY UPDATE
                    title = VALUES(title),
                    author = VALUES(author),
                    url = VALUES(url),
                    body = VALUES(body),
                    state = VALUES(state),
                    updated_at = VALUES(updated_at),
                    closed_at = VALUES(closed_at),
                    labels = VALUES(labels),
                    comments = VALUES(comments),
                    need_index = 1
            """)

            labels = issue.get('labels', [])
            label_names = [label['name'] for label in labels if 'name' in label]

            session.execute(sql, {
                'issue_id': issue['id'],
                'issue_number': issue['number'],
                'title': issue.get('title'),
                'author': issue.get('user', {}).get('login', ''),
                'url': issue.get('html_url', ''),
                'body': issue.get('body'),
                'state': issue.get('state'),
                'created_at': datetime.datetime.strptime(issue.get('created_at'), "%Y-%m-%dT%H:%M:%SZ") if issue.get
                    ('created_at') else None,
                'updated_at': datetime.datetime.strptime(issue.get('updated_at'), "%Y-%m-%dT%H:%M:%SZ") if issue.get
                    ('updated_at') else None,
                'closed_at': datetime.datetime.strptime(issue.get('closed_at'), "%Y-%m-%dT%H:%M:%SZ") if issue.get
                    ('closed_at') else None,
                'labels': json.dumps(label_names),
                'comments': issue.get('comments'),
                'connection_id': connection_id,
                'need_index': 1,
            })
            session.commit()

    def get_project_connection_by_id(self, connection_id):
        with self.db_session() as session:
            sql = """SELECT pc.id, pc.config, pc.project_id, pc.status, p.uuid as project_uuid, pc.type, pc.updated_at, pc.indexed_at
             FROM project_connection pc 
            INNER JOIN projects p 
            ON p.id=pc.project_id WHERE p.deleted=false AND pc.id=:connection_id AND pc.deleted=false"""
            connection = session.execute(text(sql), {'connection_id': connection_id}).fetchone()
        return connection

    def tag_issue_deleted(self, connection_id, issue_id):
        with self.db_session() as session:
            sql = text("""
                UPDATE github_issues
                SET deleted = 1
                WHERE connection_id = :connection_id AND issue_id = :issue_id
            """)
            session.execute(sql, {
                'connection_id': connection_id,
                'issue_id': issue_id
            })
            session.commit()

    def update_status_and_time(self, connection_id, status, updated_at):
        with self.db_session() as session:
            cmd = """UPDATE project_connection SET updated_at=:updated_at, status=:status WHERE id = :connection_id"""
            params = {'connection_id': connection_id, 'updated_at': updated_at, 'status': status}
            session.execute(text(cmd), params)
            session.commit()

    def finish_sync(self, connection_id, updated_at, connection_status, sync_count):
        total_count = self.get_github_issues_count(connection_id)
        connection_status['total_records'] = total_count
        connection_status['last_sync_count'] = sync_count
        connection_status['last_sync_status'] = CrawlStatus.COMPLETED
        self.update_status_and_time(connection_id, json.dumps(connection_status), updated_at)

    def get_github_issues_count(self, connection_id):
        with self.db_session() as session:
            sql = """SELECT count(*) FROM github_issues WHERE connection_id=:connection_id"""
            connection = session.execute(text(sql), {'connection_id': connection_id}).fetchone()
        return connection[0]

    def get_webhook_secret(self, connection_id):
        with self.db_session() as session:
            cmd = "SELECT config FROM project_connection WHERE id = :connection_id"
            result = session.execute(text(cmd), {'connection_id': connection_id}).fetchone()
            if not result:
                return None
            config_str = result[0]

            if not config_str:
                return None

            config = json.loads(config_str)
            webhook_secret = decrypt_config(config).get('webhook_secret')

            return webhook_secret

    def verify_signature(self, signature, msg, github_secret):
        if not signature:
            return True
        sha_name, signature = signature.split('=')

        if sha_name != 'sha256':
            return False
        mac = hmac.new(github_secret.encode(), msg=msg, digestmod=hashlib.sha256)

        return hmac.compare_digest(mac.hexdigest(), signature)

    def update_github_issues_by_webhook(self, connection_id, issue, issue_id, action, signature, msg):
        logger.info(f"Received webhook for issue #{issue_id}, action: {action}")
        secret = self.get_webhook_secret(connection_id)
        if not self.verify_signature(signature, msg, secret):
            raise SignatureInvalidException()

        self.save_or_update_issues_to_db(connection_id, issue)
        if action == 'deleted':
            self.tag_issue_deleted(connection_id, issue_id)
        self.add_issues_index_task(connection_id)

    def add_github_issues_sync_task(self, connection_id):
        logger.info('add github issues sync task: %s', connection_id)
        msg_content = {
            'connection_id': connection_id,
        }
        self.mq.lpush('issues_sync_task', json.dumps(msg_content))

    def issues_sync(self, project_connection):
        logger.info(f"Start crawling site {project_connection.id}")
        connection_id = project_connection.id
        connection_status = json.loads(project_connection.status)
        connection_status['last_sync_status'] = CrawlStatus.CRAWLING

        self.update_issues_sync_status(connection_id, json.dumps(connection_status))

        config = decrypt_config(json.loads(project_connection.config))
        server_url = config.get('repository')
        token = config.get('token')
        old_updated_at = project_connection.updated_at
        try:
            path = urlparse(server_url).path
            parts = path.strip("/").split("/")
            repo_owner, repo_name = parts[0], parts[1]
        except Exception as e:
            raise GithubRepositoryInvalidException("github repository is invalid: %s", e)

        github_api = GitHubAPI(token, repo_owner, repo_name, connection_id)
        since = old_updated_at.isoformat() + "Z" if old_updated_at else None
        new_updated_at = datetime.datetime.now(datetime.UTC)

        page = 1
        per_page = 100  # max 100
        sync_count = 0
        while True:
            issues = github_api.list_issues(since=since, state='all', per_page=per_page, page=page)
            if not issues:
                break

            for issue in issues:
                if 'pull_request' in issue:
                    continue
                sync_count += 1
                self.save_or_update_issues_to_db(connection_id, issue)
                if need_update_connection_status(sync_count):
                    connection_status['last_sync_count'] = sync_count
                    self.update_issues_sync_status(connection_id, json.dumps(connection_status))

            if len(issues) < per_page:
                break
            page += 1
        logger.info(f"Fetched {sync_count} issues from GitHub")

        self.finish_sync(connection_id, new_updated_at, connection_status, sync_count)
        logger.info('Finish github issues sync task: %s', connection_id)
        self.add_issues_index_task(connection_id)

    def update_issues_sync_status(self, connection_id, status):
        with self.db_session() as session:
            cmd = """UPDATE project_connection SET status = :status WHERE id = :connection_id"""
            session.execute(text(cmd), {'connection_id': connection_id, 'status': status})
            session.commit()

    def update_issues_sync(self):
        """Check if all issues need to be updated"""
        start, limit = 0, 1000
        while True:
            try:
                project_connections = self.get_project_connection_by_page(start, limit)
            except Exception as e:
                logger.error("Error: %s" % e)
                return
            start += 1000
            if len(project_connections) == 0:
                break
            for project_connection in project_connections:
                connection_id = project_connection.id
                try:
                    self.add_github_issues_sync_task(connection_id)
                except Exception as e:
                    logger.warning('update issues sync failed, connection_id: %s, error: %s', connection_id, e)
                    sync_status = json.loads(project_connection.status)
                    sync_status['last_sync_status'] = CrawlStatus.FAILED
                    self.update_issues_sync_status(connection_id, json.dumps(sync_status))

    def get_project_connection_by_page(self, start, limit):
        with self.db_session() as session:
            sql = """SELECT pc.id, pc.config, pc.project_id, pc.status, p.uuid
             FROM project_connection pc 
            INNER JOIN projects p 
            ON p.id=pc.project_id WHERE p.deleted=false AND pc.type='github_issue' LIMIT :start, :limit"""
            project_issues = session.execute(text(sql), {'start': start, 'limit': limit}).fetchall()
            return project_issues

    def add_issues_index_task(self, connection_id):
        msg_content = {
            'connection_id': connection_id,
        }
        self.mq.lpush('index_task', json.dumps(msg_content))
