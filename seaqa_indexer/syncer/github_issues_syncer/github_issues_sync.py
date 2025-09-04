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
                    issue_id, issue_number, title, body, state,
                    created_at, updated_at, closed_at, labels,
                    comments_count, connection_id, author, url, need_index
                ) VALUES (
                    :issue_id, :issue_number, :title, :body, :state,
                    :created_at, :updated_at, :closed_at, :labels,
                    :comments_count, :connection_id, :author, :url, :need_index
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
                    comments_count = VALUES(comments_count),
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
                'comments_count': issue.get('comments'),
                'connection_id': connection_id,
                'need_index': 1,
            })
            session.commit()

    def save_or_update_issue_comments_to_db(self, connection_id, issue_id, comment):
        with self.db_session_class() as session:
            sql = text("""
                    INSERT INTO github_issue_comments (
                        comment_id, issue_id, connection_id,
                        author, body, created_at, updated_at
                    ) VALUES (
                        :comment_id, :issue_id, :connection_id,
                        :author, :body, :created_at, :updated_at
                    )
                    ON DUPLICATE KEY UPDATE
                        author = VALUES(author),
                        body = VALUES(body),
                        updated_at = VALUES(updated_at) ;
                """)

            created_at = datetime.datetime.strptime(comment['created_at'], "%Y-%m-%dT%H:%M:%SZ") if comment.get(
                'created_at') else None
            updated_at = datetime.datetime.strptime(comment['updated_at'], "%Y-%m-%dT%H:%M:%SZ") if comment.get(
                'updated_at') else None

            session.execute(sql, {
                'comment_id': comment['id'],
                'issue_id': issue_id,
                'connection_id': connection_id,
                'author': comment.get('user', {}).get('login', ''),
                'body': comment.get('body', ''),
                'created_at': created_at,
                'updated_at': updated_at,
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

    def tag_comment_deleted(self, connection_id, comment_id):
        with self.db_session_class() as session:
            sql = text("""
                UPDATE github_issue_comments
                SET deleted = 1
                WHERE connection_id = :connection_id AND comment_id = :comment_id
            """)
            session.execute(sql, {
                'connection_id': connection_id,
                'comment_id': comment_id
            })
            session.commit()

    def update_issue_record(self, connection_id, issue_id, updated_at, comments_count):
        with self.db_session_class() as session:
            sql = text("""
                UPDATE github_issues
                SET need_index = 1, updated_at = :updated_at, comments_count = :comments_count
                WHERE connection_id = :connection_id AND issue_id = :issue_id
            """)

            updated_time = datetime.datetime.strptime(updated_at, "%Y-%m-%dT%H:%M:%SZ") if updated_at else None,

            session.execute(sql, {
                'connection_id': connection_id,
                'issue_id': issue_id,
                'updated_at': updated_time,
                'comments_count': comments_count,
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
                comments_page = 1
                while True:
                    comments = github_api.get_issue_comments(issue['number'], per_page=per_page, page=comments_page)
                    comments_page += 1
                    if not comments:
                        break
                    for comment in comments:
                        sync_count += 1
                        self.save_or_update_issue_comments_to_db(connection_id, issue['id'], comment)

            if len(issues) < per_page:
                return sync_count
            page += 1
