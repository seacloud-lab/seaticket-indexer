import logging
import json

from soupsieve.css_types import pickle_register
from sqlalchemy.sql import text

from seaqa_indexer.seadb_models.github_seadb_api import GitHubSeaDBAPI
from seaqa_indexer.seadb_models.models import GithubIssuesTable, GithubIssueCommentsTable
from seaqa_indexer.utils import need_update_connection_status, time_str_to_utc_time
from seaqa_indexer.utils.github_issues_api import GitHubAPI
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.utils.seaqa_db import SeaqaDB

logger = logging.getLogger(__name__)


MAX_BODY_LENGTH = 10000


class GithubIssuesSync:
    def __init__(self,project_uuid, db_session_class):
        self.db_session_class = db_session_class
        self.seadb_api = SeaDBAPI()
        self.seaqa_db = SeaqaDB(db_session_class)
        self.github_seadb_api = GitHubSeaDBAPI(project_uuid)
        self.issues_table = GithubIssuesTable
        self.comments_table = GithubIssueCommentsTable

    def get_current_table_metadata(self, tables, table_name):
        for table in tables:
            if table['name'] == table_name:
                return table
        return None

    def update_connection_status(self, connection_id, status):
        with self.db_session_class() as session:
            cmd = """UPDATE project_connection SET status = :status WHERE id = :connection_id"""
            session.execute(text(cmd), {'connection_id': connection_id, 'status': status})
            session.commit()

    def save_or_update_issue_to_db(self, connection_id, issue):
        labels_data = issue.get('labels', [])
        label_names = [label['name'] for label in labels_data if 'name' in label]

        assignees_data = issue.get('assignees', [])
        assignees_list = [assignee['login'] for assignee in assignees_data if 'login' in assignee]

        issue_type = issue.get('type', None)
        if issue_type:
            type_name = issue_type['name']
        else:
            type_name = None

        issue_id = issue['id']
        issue_number = issue['number']
        title = issue.get('title')
        author = issue.get('user', {}).get('login', '')
        url = issue.get('html_url', '')
        body = issue.get('body')
        state = issue.get('state')
        state_reason = issue.get('state_reason')
        assignees = json.dumps(assignees_list)
        created_at = time_str_to_utc_time(issue.get('created_at')).isoformat()
        updated_at = time_str_to_utc_time(issue.get('updated_at')).isoformat()
        closed_at = time_str_to_utc_time(issue.get('closed_at')).isoformat() if issue.get('closed_at') else None
        comments_count = issue.get('comments')
        deleted = False

        if body and len(body) > MAX_BODY_LENGTH:
            body = body[:MAX_BODY_LENGTH]

        insert_row = {
            self.issues_table.issue_id.name:issue_id,
            self.issues_table.issue_number.name: issue_number,
            self.issues_table.title.name: title,
            self.issues_table.author.name: author,
            self.issues_table.url.name: url,
            self.issues_table.body.name: body,
            self.issues_table.state.name: state,
            self.issues_table.assignees.name: assignees,
            self.issues_table.created_at.name: created_at,
            self.issues_table.updated_at.name: updated_at,
            self.issues_table.closed_at.name: closed_at,
            self.issues_table.comments_count.name: comments_count,
            self.issues_table.deleted.name: deleted,
        }

        if state_reason is not None:
            insert_row[self.issues_table.state_reason.name] = state_reason

        if label_names is not None:
            insert_row[self.issues_table.labels.name] = label_names

        if type_name is not None:
            insert_row[self.issues_table.issue_type.name] = type_name

        res = self.github_seadb_api.query_issue(connection_id, issue_id)
        if res:
            _pk = res[0].get('_pk')
            update_row = [
                {
                "pk":_pk,
                "row":insert_row,
                }
            ]
            self.github_seadb_api.update_issue_to_db(connection_id, update_row)
        else:
            self.github_seadb_api.save_issue_to_db(connection_id, insert_row)

    def save_or_update_issue_comment_to_db(self, connection_id, issue_id, comment):

        created_at = time_str_to_utc_time(comment.get('created_at')).isoformat()
        updated_at = time_str_to_utc_time(comment.get('updated_at')).isoformat()
        comment_id = comment['id']
        issue_id = int(issue_id)
        author = comment.get('user', {}).get('login', '')
        body = comment.get('body', '')
        if body and len(body) > MAX_BODY_LENGTH:
            body = body[:MAX_BODY_LENGTH]

        insert_row = {
            GithubIssueCommentsTable.comment_id.name: comment_id,
            GithubIssueCommentsTable.issue_id.name: issue_id,
            GithubIssueCommentsTable.author.name: author,
            GithubIssueCommentsTable.body.name: body,
            GithubIssueCommentsTable.created_at.name: created_at,
            GithubIssueCommentsTable.updated_at.name: updated_at,
        }

        res = self.github_seadb_api.query_issue_comment(connection_id, comment_id)
        if res:
            _pk = res[0].get('_pk')
            update_row = [
                {
                "pk":_pk,
                "row":insert_row,
                }
            ]
            self.github_seadb_api.update_issue_comment_to_db(connection_id, update_row)
        else:
            self.github_seadb_api.save_issue_comment_to_db(connection_id, insert_row)

    def update_issue_record(self, connection_id, issue_id, updated_at, comments_count=None, deleted=False):
        updated_at = time_str_to_utc_time(updated_at).isoformat()
        row = {
            GithubIssuesTable.updated_at.name: updated_at,
        }

        if comments_count is not None:
            comments_count = int(comments_count)
            row[GithubIssuesTable.comments_count.name] = comments_count
        if deleted:
            row[GithubIssuesTable.deleted.name] = deleted
        self.github_seadb_api.update_issue_record(connection_id, issue_id, row)

    def sync(self, project_uuid, connection_id, token, repo_owner, repo_name, since, connection_status):
        page = 1
        per_page = 100  # max 100
        sync_count = 0
        github_api = GitHubAPI(token, repo_owner, repo_name, connection_id)
        table_name = str(connection_id) + '_github_issues'
        metadata = self.seadb_api.get_base_metadata(project_uuid)
        tables_metadata = metadata.get('tables') or []
        issues_labels = github_api.get_issues_labels(page, per_page)
        table_metadata = self.get_current_table_metadata(tables_metadata, str(table_name))
        table_id = table_metadata.get('id')
        columns_metadata = table_metadata.get("columns", [])

        for issues_label in issues_labels:
            for column in columns_metadata:
                if column.get('name') == 'labels':
                    if column['data'] is not None:
                        option_names = {opt['name'] for opt in column['data']['options']}
                    else:
                        option_names = None
                    if option_names is None or issues_label.get('name') not in option_names:
                        column_key = column.get('key')
                        column_option = {
                            'table_id': table_id,
                            'column_key': column_key,
                            'option_name': issues_label.get('name'),
                        }
                        try:
                            self.seadb_api.add_column_option(project_uuid, column_option)
                        except Exception as e:
                            logging.error(e)
                            return sync_count

        while True:
            issues = github_api.list_issues(since=since, state='all', per_page=per_page, page=page)
            if not issues:
                return sync_count

            for issue in issues:
                if 'pull_request' in issue:
                    continue
                sync_count += 1
                self.save_or_update_issue_to_db(connection_id, issue)
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
                        self.save_or_update_issue_comment_to_db(connection_id, issue['id'], comment)

            if len(issues) < per_page:
                return sync_count
            page += 1
