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
from seaqa_indexer.utils import decrypt_config
from seaqa_indexer.utils.constants import CrawlStatus
from seaqa_indexer.utils.github_issues_api import GitHubAPI

logger = logging.getLogger(__name__)


class SignatureInvalidException(Exception):
    pass


class GithubIssuesSyncManager:
    def __init__(self):
        self.db_session = init_db_session_class()
        self.mq = get_mq(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)

    def save_or_update_issues_to_db(self, issues_sync_id, repo_owner, repo_name, issue):
        with self.db_session() as session:
            sql = text("""
                INSERT INTO github_issues (
                    issue_id, repo_owner, repo_name, issue_number, title, body, state,
                    created_at, updated_at, closed_at, labels,
                    comments, connection_id, author, url, need_index
                ) VALUES (
                    :issue_id, :repo_owner, :repo_name, :issue_number, :title, :body, :state,
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
                'repo_owner': repo_owner,
                'repo_name': repo_name,
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
                'connection_id': str(issues_sync_id),
                'need_index': 1,
            })
            session.commit()

    def get_issues_connection_by_id(self, issues_sync_id):
        with self.db_session() as session:
            sql = """SELECT pc.id, pc.config, pc.project_id, pc.status, p.uuid as project_uuid, pc.type, pc.updated_at, pc.indexed_at
             FROM project_connection pc 
            INNER JOIN projects p 
            ON p.id=pc.project_id WHERE p.deleted=false AND pc.id=:connection_id AND pc.deleted=false"""
            connection = session.execute(text(sql), {'connection_id': issues_sync_id}).fetchone()
        return connection

    def tag_issue_deleted(self, issues_sync_id, repo_owner, repo_name, issue_number):
        with self.db_session() as session:
            sql = text("""
                UPDATE github_issues
                SET deleted = 1
                WHERE connection_id = :connection_id AND repo_owner = :repo_owner AND issue_number = :issue_number
            """)
            session.execute(sql, {
                'connection_id': issues_sync_id,
                'repo_owner': repo_owner,
                'repo_name': repo_name,
                'issue_number': issue_number
            })
            session.commit()

    def finish_sync(self, issues_sync_id, updated_at, status):
        with self.db_session() as session:
            cmd = """UPDATE project_connection SET updated_at=:updated_at, status=:status WHERE id = :connection_id"""
            params = {'connection_id': issues_sync_id, 'updated_at': updated_at,  'status': status}
            session.execute(text(cmd), params)
            session.commit()

    def get_webhook_secret(self, issues_sync_id):
        with self.db_session() as session:
            cmd = "SELECT config FROM project_connection WHERE id = :connection_id"
            result = session.execute(text(cmd), {'connection_id': issues_sync_id}).fetchone()
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

    def update_github_issues_by_webhook(self, issues_sync_id, issue, repo_owner, repo_name, issue_number, action, signature, msg):
        logger.debug(f"Received webhook for issue #{issue_number}, action: {action}")
        secret = self.get_webhook_secret(issues_sync_id)
        if not self.verify_signature(signature, msg, secret):
            raise SignatureInvalidException()

        self.save_or_update_issues_to_db(issues_sync_id, repo_owner, repo_name, issue)
        if action == 'deleted':
            self.tag_issue_deleted(issues_sync_id, repo_owner, repo_name, issue_number)
        new_updated_at = datetime.datetime.now(datetime.UTC)
        self.add_issues_index_task(issues_sync_id)
        self.finish_sync(issues_sync_id, new_updated_at, CrawlStatus.COMPLETED)

    def add_github_issues_sync_task(self, connection_id):
        logger.info('add github issues sync task: %s', connection_id)
        msg_content = {
            'connection_id': connection_id,
        }
        self.mq.lpush('issues_sync_task', json.dumps(msg_content))

    def issues_sync(self, issues_sync_id):
        print(66666)
        project_connection = self.get_issues_connection_by_id(issues_sync_id)
        issues_sync_status = project_connection.status
        if issues_sync_status == CrawlStatus.CRAWLING:
            logger.warning('issues: %s is crawling', issues_sync_id)
            return
        self.update_issues_sync_status(issues_sync_id, CrawlStatus.CRAWLING)
        config = decrypt_config(json.loads(project_connection.config))
        server_url = config.get('repository')
        token = config.get('token')
        old_last_modified = project_connection.updated_at
        try:
            path = urlparse(server_url).path
            parts = path.strip("/").split("/")
            repo_owner, repo_name = parts[0], parts[1]
        except Exception as e:
            raise Exception("github repository is invalid: %s", e)

        github_api = GitHubAPI(token, repo_owner, repo_name, issues_sync_id)
        since = old_last_modified.isoformat() + "Z" if old_last_modified else None
        new_updated_at = datetime.datetime.now(datetime.UTC)

        list_all_issues = github_api.list_all_issues(since=None)

        for issue in list_all_issues:
            print(111)
            if 'pull_request' in issue:
                continue
            self.save_or_update_issues_to_db(issues_sync_id, repo_owner, repo_name, issue)

        self.finish_sync(issues_sync_id, new_updated_at, CrawlStatus.COMPLETED)
        logger.info('Finish github issues sync task: %s', issues_sync_id)
        self.add_issues_index_task(issues_sync_id)

    def update_issues_sync_status(self, issues_sync_id, status):
        with self.db_session() as session:
            cmd = """UPDATE project_connection SET status = :status WHERE id = :issues_sync_id"""
            session.execute(text(cmd), {'issues_sync_id': issues_sync_id, 'status': status})
            session.commit()

    def update_issues_sync(self):
        """Check if all issues need to be updated"""
        start, limit = 0, 1000
        while True:
            try:
                project_issues = self.get_project_issues_by_page(start, limit)
            except Exception as e:
                logger.error("Error: %s" % e)
                return
            start += 1000
            if len(project_issues) == 0:
                break
            for issue in project_issues:
                issues_sync_id = issue.issues_sync_id
                try:
                    self.issues_sync(issues_sync_id)
                except Exception as e:
                    logger.warning('update issues sync failed, issues_sync_id: %s, error: %s', issues_sync_id, e)
                    self.update_issues_sync_status(issues_sync_id, CrawlStatus.FAILED)

    def get_project_issues_by_page(self, start, limit):
        with self.db_session() as session:
            sql = """SELECT pc.id as issues_sync_id, pc.config, pc.project_id, pc.status, p.uuid
             FROM project_connection pc 
            INNER JOIN projects p 
            ON p.id=pc.project_id WHERE p.deleted=false AND pc.type='github_issue' LIMIT :start, :limit"""
            project_issues = session.execute(text(sql), {'start': start, 'limit': limit}).fetchall()
            return project_issues

    def add_issues_index_task(self, issues_sync_id):
        msg_content = {
            'connection_id': issues_sync_id,
        }
        self.mq.lpush('index_task', json.dumps(msg_content))
