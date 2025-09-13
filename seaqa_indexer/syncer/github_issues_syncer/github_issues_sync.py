import logging
import json

from sqlalchemy.sql import text

from seaqa_indexer.utils import need_update_connection_status
from seaqa_indexer.utils.constants import GITHUB_ISSUES_TABLE, GITHUB_ISSUES_COMMENTS_TABLE
from seaqa_indexer.utils.github_issues_api import GitHubAPI
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.utils.seaqa_db import SeaqaDB

logger = logging.getLogger(__name__)


class GithubIssuesSync:
    def __init__(self, db_session_class):
        self.db_session_class = db_session_class
        self.seadb_api = SeaDBAPI()
        self.seaqa_db = SeaqaDB(db_session_class)

    def save_or_update_issues_to_db(self,project_uuid, connection_id, issue, is_webhook):
        labels = issue.get('labels', [])
        label_names = [label['name'] for label in labels if 'name' in label]

        issue_id = issue['id']
        issue_number = issue['number']
        title = issue.get('title')
        author = issue.get('user', {}).get('login', '')
        url = issue.get('html_url', '')
        body = issue.get('body')
        state = issue.get('state')
        created_at = issue.get('created_at') if 'created_at' in issue else None
        updated_at = issue.get('updated_at') if issue.get('updated_at') else None
        closed_at = issue.get('closed_at') if issue.get('closed_at') else None
        labels = json.dumps(label_names)
        comments_count = issue.get('comments')
        connection_id = int(connection_id)
        need_index = True
        deleted = False
        insert_rows = [
            {
                GITHUB_ISSUES_TABLE.columns.issue_id.name:issue_id,
                GITHUB_ISSUES_TABLE.columns.issue_number.name: issue_number,
                GITHUB_ISSUES_TABLE.columns.title.name: title,
                GITHUB_ISSUES_TABLE.columns.author.name: author,
                GITHUB_ISSUES_TABLE.columns.url.name: url,
                GITHUB_ISSUES_TABLE.columns.body.name: body,
                GITHUB_ISSUES_TABLE.columns.state.name: state,
                GITHUB_ISSUES_TABLE.columns.created_at.name: created_at,
                GITHUB_ISSUES_TABLE.columns.updated_at.name: updated_at,
                GITHUB_ISSUES_TABLE.columns.closed_at.name: closed_at,
                GITHUB_ISSUES_TABLE.columns.labels.name: labels,
                GITHUB_ISSUES_TABLE.columns.comments_count.name: comments_count,
                GITHUB_ISSUES_TABLE.columns.connection_id.name: connection_id,
                GITHUB_ISSUES_TABLE.columns.need_index.name: need_index,
                GITHUB_ISSUES_TABLE.columns.deleted.name: deleted,
            }
        ]
        table_name = str(connection_id) + '_github_issues'
        if is_webhook:
            query_sql = f"SELECT * FROM `{table_name}` WHERE connection_id={connection_id} AND issue_id={issue_id}"
            res = self.seadb_api.query_rows(project_uuid, query_sql).get('results', [])
            if res:
                _pk = res[0].get('_pk')
                update_rows = [
                    {
                    "pk":_pk,
                    "row":insert_rows[0],
                    }
                ]
                self.seadb_api.update_rows(project_uuid, table_name, update_rows)
            else:
                self.seadb_api.insert_rows(project_uuid, table_name, insert_rows)
        else:
            self.seadb_api.insert_rows(project_uuid, table_name, insert_rows)

    def save_or_update_issue_comments_to_db(self, connection_id, issue_id, comment, is_webhook):
        project_connection = self.seaqa_db.get_project_connection_by_id(connection_id)
        project_uuid = project_connection.project_uuid
        table_name = str(connection_id) + '_issue_comments'
        created_at = comment.get('created_at') if 'created_at' in comment else None
        updated_at = comment.get('updated_at') if comment.get('updated_at') else None
        comment_id = comment['id']
        issue_id = int(issue_id)
        connection_id = int(connection_id)
        author = comment.get('user', {}).get('login', '')
        body = comment.get('body', '')
        created_at = created_at
        updated_at = updated_at
        insert_rows = [
            {
                GITHUB_ISSUES_COMMENTS_TABLE.columns.comment_id.name: comment_id,
                GITHUB_ISSUES_COMMENTS_TABLE.columns.issue_id.name: issue_id,
                GITHUB_ISSUES_COMMENTS_TABLE.columns.connection_id.name: connection_id,
                GITHUB_ISSUES_COMMENTS_TABLE.columns.author.name: author,
                GITHUB_ISSUES_COMMENTS_TABLE.columns.body.name: body,
                GITHUB_ISSUES_COMMENTS_TABLE.columns.created_at.name: created_at,
                GITHUB_ISSUES_COMMENTS_TABLE.columns.updated_at.name: updated_at,
            }
        ]

        if is_webhook:
            query_sql = f"SELECT * FROM `{table_name}` WHERE connection_id={connection_id} AND issue_id={issue_id}"
            res = self.seadb_api.query_rows(project_uuid, query_sql).get('results', [])
            if res:
                _pk = res[0].get('_pk')
                update_rows = [
                    {
                    "pk":_pk,
                    "row":insert_rows[0],
                    }
                ]
                self.seadb_api.update_rows(project_uuid, table_name, update_rows)
            else:
                self.seadb_api.insert_rows(project_uuid, table_name, insert_rows)
        else:
            self.seadb_api.insert_rows(project_uuid,table_name, insert_rows)

    def tag_comment_deleted(self,project_uuid, connection_id, comment_id):
        table_name = str(connection_id) + '_issue_comments'
        sql = f"SELECT * FROM `{table_name}` WHERE comment_id = {comment_id}"
        issue = self.seadb_api.query_rows(project_uuid, sql).get('results', [])
        _pk = issue[0].get('_pk')
        update_rows = [
                {
                "pk":_pk,
                "row":
                    {
                        GITHUB_ISSUES_COMMENTS_TABLE.columns.deleted.name: True,
                    }
                }
            ]
        self.seadb_api.update_rows(project_uuid, table_name, update_rows)

    def tag_issue_deleted(self,project_uuid, connection_id, issue_id):
        table_name = str(connection_id) + '_github_issues'
        sql = f"SELECT * FROM `{table_name}` WHERE issue_id = {issue_id}"
        issue = self.seadb_api.query_rows(project_uuid, sql).get('results', [])
        _pk = issue[0].get('_pk')
        update_rows = [
            {
            "pk":_pk,
            "row":
                {
                    GITHUB_ISSUES_COMMENTS_TABLE.columns.deleted.name: True,
                }
            }
        ]
        self.seadb_api.update_rows(project_uuid, table_name, update_rows)

    def update_issue_record(self, connection_id, issue_id, updated_at, comments_count):
        project_connection = self.seaqa_db.get_project_connection_by_id(connection_id)
        project_uuid = project_connection.project_uuid
        table_name = str(connection_id) + '_github_issues'
        sql = f"SELECT * FROM `{table_name}` WHERE issue_id = {issue_id}"
        issue = self.seadb_api.query_rows(project_uuid, sql).get('results', [])
        _pk = issue[0].get('_pk')
        comments_count = int(comments_count)
        update_rows = [
            {
                "pk": _pk,
                "row":
                    {
                        GITHUB_ISSUES_TABLE.columns.updated_at.name: updated_at,
                        GITHUB_ISSUES_TABLE.columns.comments_count.name: comments_count,
                        GITHUB_ISSUES_TABLE.columns.need_index.name: True,
                    }
            }
        ]
        self.seadb_api.update_rows(project_uuid, table_name, update_rows)

    def get_github_issues_count(self, connection_id):
        project_connection = self.seaqa_db.get_project_connection_by_id(connection_id)
        project_uuid = project_connection.project_uuid
        table_name = str(connection_id) + '_github_issues'
        sql = f"SELECT count(*) FROM `{table_name}`"
        issues_count = self.seadb_api.query_rows(project_uuid, sql).get('results', [])[0]
        return issues_count

    def update_connection_status(self, connection_id, status):
        with self.db_session_class() as session:
            cmd = """UPDATE project_connection SET status = :status WHERE id = :connection_id"""
            session.execute(text(cmd), {'connection_id': connection_id, 'status': status})
            session.commit()

    def sync(self, project_uuid, connection_id, token, repo_owner, repo_name, since, connection_status):
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
                self.save_or_update_issues_to_db(project_uuid, connection_id, issue, is_webhook=False)
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
                        self.save_or_update_issue_comments_to_db(connection_id, issue['id'], comment, is_webhook=False)

            if len(issues) < per_page:
                return sync_count
            page += 1
