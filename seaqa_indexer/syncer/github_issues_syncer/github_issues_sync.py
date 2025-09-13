import datetime
import logging
import json

from sqlalchemy.sql import text

from seaqa_indexer.utils import need_update_connection_status
from seaqa_indexer.utils.github_issues_api import GitHubAPI

logger = logging.getLogger(__name__)


class GithubIssuesSync:
    def __init__(self, db_session_class):
        self.db_session_class = db_session_class

    def save_or_update_issues_to_db(self, connection_id, issue):
        with self.db_session_class() as session:
            sql = text("""
                INSERT INTO github_issues (
                    issue_id, issue_number, title, body, state, state_reason,
                    created_at, updated_at, closed_at, labels, assignees, issue_type,
                    comments, connection_id, author, url, need_index
                ) VALUES (
                    :issue_id, :issue_number, :title, :body, :state, :state_reason,
                    :created_at, :updated_at, :closed_at, :labels, :assignees, :issue_type,
                    :comments, :connection_id, :author, :url, :need_index
                )
                ON DUPLICATE KEY UPDATE
                    title = VALUES(title),
                    author = VALUES(author),
                    url = VALUES(url),
                    body = VALUES(body),
                    assignees = VALUES(assignees),
                    issue_type = VALUES(issue_type),
                    state = VALUES(state),
                    state_reason = VALUES(state_reason),
                    updated_at = VALUES(updated_at),
                    closed_at = VALUES(closed_at),
                    labels = VALUES(labels),
                    comments = VALUES(comments),
                    need_index = 1
            """)

            labels = issue.get('labels', [])
            label_names = [label['name'] for label in labels if 'name' in label]

            assignees = issue.get('assignees', [])
            assignees_list = [assignee['login'] for assignee in assignees if 'login' in assignee]

            types = issue.get('type', None)
            if types:
                type_names = types['name']
            else:
                type_names = None

            session.execute(sql, {
                'issue_id': issue['id'],
                'issue_number': issue['number'],
                'title': issue.get('title'),
                'author': issue.get('user', {}).get('login', ''),
                'url': issue.get('html_url', ''),
                'body': issue.get('body'),
                'state': issue.get('state'),
                'state_reason': issue.get('state_reason'),
                'created_at': datetime.datetime.strptime(issue.get('created_at'), "%Y-%m-%dT%H:%M:%SZ") if issue.get
                    ('created_at') else None,
                'updated_at': datetime.datetime.strptime(issue.get('updated_at'), "%Y-%m-%dT%H:%M:%SZ") if issue.get
                    ('updated_at') else None,
                'closed_at': datetime.datetime.strptime(issue.get('closed_at'), "%Y-%m-%dT%H:%M:%SZ") if issue.get
                    ('closed_at') else None,
                'labels': json.dumps(label_names),
                'assignees': json.dumps(assignees_list),
                'issue_type': type_names,
                'comments': issue.get('comments'),
                'connection_id': connection_id,
                'need_index': 1,
            })
            session.commit()

    def tag_issue_deleted(self, connection_id, issue_id):
        with self.db_session_class() as session:
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

    def get_github_issues_count(self, connection_id):
        with self.db_session_class() as session:
            sql = """SELECT count(*) FROM github_issues WHERE connection_id=:connection_id"""
            connection = session.execute(text(sql), {'connection_id': connection_id}).fetchone()
        return connection[0]

    def update_connection_status(self, connection_id, status):
        with self.db_session_class() as session:
            cmd = """UPDATE project_connection SET status = :status WHERE id = :connection_id"""
            session.execute(text(cmd), {'connection_id': connection_id, 'status': status})
            session.commit()

    def sync(self, connection_id, token, repo_owner, repo_name, since, connection_status):
        page = 1
        per_page = 100  # max 100
        sync_count = 0
        github_api = GitHubAPI(token, repo_owner, repo_name, connection_id)
        while True:
            issues = github_api.list_issues(since=since, state='all', per_page=per_page, page=page)
            if not issues:
                return sync_count

            for issue in issues:
                if 'pull_request' in issue:
                    continue
                sync_count += 1
                self.save_or_update_issues_to_db(connection_id, issue)
                if need_update_connection_status(sync_count):
                    connection_status['last_sync_count'] = sync_count
                    self.update_connection_status(connection_id, json.dumps(connection_status))

            if len(issues) < per_page:
                return sync_count
            page += 1
