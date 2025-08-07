import datetime
import logging
import json

from urllib.parse import urlparse

from sqlalchemy.sql import text

from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
from seaqa_indexer.mq import get_mq
from seaqa_indexer.utils import decrypt_config
from seaqa_indexer.utils.constants import CrawlStatus
from seaqa_indexer.utils.github_issues_api import GitHubAPI
from seaqa_indexer.seasearch.index_manager import IndexManager

logger = logging.getLogger(__name__)

class GithubIssuesSyncManager:
    def __init__(self):
        self.db_session = init_db_session_class()
        self.mq = get_mq(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)
        self.index_manager = IndexManager()


    def save_issues_to_db(self, issues_sync_id, repo_owner, repo_name, issue):
        with self.db_session() as session:
            sql = text("""
                INSERT INTO github_issues (
                    id, repo_owner, repo_name, issue_number, title, body, state,
                    created_at, updated_at, closed_at, labels,
                    comments, issues_sync_id, author, url, need_index
                ) VALUES (
                    :id, :repo_owner, :repo_name, :issue_number, :title, :body, :state,
                    :created_at, :updated_at, :closed_at, :labels,
                    :comments, :issues_sync_id, :author, :url, :need_index
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
                'id': issue['id'],
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
                'issues_sync_id': str(issues_sync_id),
                'need_index': 1,
            })
            session.commit()

    def get_issues_config_by_id(self, issues_sync_id):
        with self.db_session() as session:
            sql = """SELECT pc.id, pc.config, pc.project_id, pc.status, p.uuid as project_uuid, pc.type, pc.updated_at, pc.indexed_at
             FROM project_connection pc 
            INNER JOIN projects p 
            ON p.id=pc.project_id WHERE p.deleted=false AND pc.id=:issues_sync_id AND pc.deleted=false"""
            connection = session.execute(text(sql), {'issues_sync_id': issues_sync_id}).fetchone()
        return connection

    def delete_issues_from_db(self, ids):
        with self.db_session() as session:
            sql = text("DELETE FROM github_issues WHERE id = :id")
            for id_issue in ids:
                session.execute(sql, {'id': id_issue})
            session.commit()

    def get_issues_sync_id_by_issue_info(self, repo_name, repo_owner):
        with self.db_session() as session:
            sql = """
                SELECT issues_sync_id
                FROM github_issues
                WHERE repo_owner = :repo_owner
                AND repo_name = :repo_name
                LIMIT 1
            """
            result = session.execute(
                text(sql),
                {
                    'repo_owner': repo_owner,
                    'repo_name': repo_name,
                }
            ).fetchone()

        return result.issues_sync_id if result else None

    def get_webhook_secret(self, issues_sync_id):
        with self.db_session() as session:
            cmd = "SELECT config FROM project_connection WHERE id = :issues_sync_id"
            result = session.execute(text(cmd), {'issues_sync_id': issues_sync_id}).fetchone()
            if not result:
                return None
            config_str = result[0]

            if not config_str:
                return None

            config = json.loads(config_str)

            return config.get('webhook_secret')

    def update_github_issues_by_webhook(self,issues_sync_id, issue, repo_owner, repo_name):
        self.save_issues_to_db(issues_sync_id, repo_owner, repo_name, issue)
        new_updated_at = datetime.datetime.now(datetime.UTC)
        self.add_issues_index_task(issues_sync_id)
        self.index_manager.finish_index(issues_sync_id,indexed_at=None, updated_at=new_updated_at)

    def add_github_issues_sync_task(self, issues_sync_id):
        logger.info('add github issues sync task: %s', issues_sync_id)
        msg_content = {
            'issues_sync_id': issues_sync_id,
        }
        self.mq.lpush('issues_sync_task', json.dumps(msg_content))

    def issues_sync(self, issues_sync_id):
        project_connection = self.get_issues_config_by_id(issues_sync_id)
        config = decrypt_config(json.loads(project_connection.config))
        server_url = config.get('repository')
        token = config.get('token')
        old_last_modified = project_connection.updated_at
        repo_owner, repo_name = "", ""
        try:
            path = urlparse(server_url).path
            parts = path.strip("/").split("/")
            if len(parts) >= 3:
                repo_owner, repo_name = parts[0], parts[1]

        except Exception as e:
            logger.error(f"URL format is invalid: {e}")

        github_api = GitHubAPI(token, repo_owner, repo_name, issues_sync_id)
        since = old_last_modified.isoformat() + "Z" if old_last_modified else None
        new_updated_at = datetime.datetime.now(datetime.UTC)
        self.update_issues_sync_status(issues_sync_id, CrawlStatus.CRAWLING)
        for issue in github_api.list_all_issues(since=since):
            if 'pull_request' in issue:
                continue
            self.save_issues_to_db(issues_sync_id, repo_owner, repo_name, issue)
        self.index_manager.finish_index(issues_sync_id,indexed_at=None, updated_at=new_updated_at)
        self.add_issues_index_task(issues_sync_id)
        self.update_issues_sync_status(issues_sync_id, CrawlStatus.COMPLETED)

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