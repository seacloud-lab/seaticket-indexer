import datetime
import json
from seaqa_indexer.utils import time_to_utc_time
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.seadb_models.models import GithubIssuesTable, GithubIssueCommentsTable

MAX_CONTENT_LENGTH = 10000


class GitHubSeaDBAPI:
    def __init__(self, base_id):
        self.base_id = base_id
        self.seadb_api = SeaDBAPI()

    def tag_comment_deleted(self, connection_id, comment_id):
        table_name = GithubIssueCommentsTable.gen_table_name(connection_id)
        sql = f"UPDATE `{table_name}` SET deleted = True WHERE comment_id = {comment_id}"
        self.seadb_api.query_rows(self.base_id, sql)

    def update_issue_record(self, connection_id, issue_id, modified_time, comments_count=None, deleted=False):
        row = {
            GithubIssuesTable.modified_time.name: time_to_utc_time(modified_time).isoformat(),
        }

        if comments_count is not None:
            row[GithubIssuesTable.comments_count.name] = int(comments_count)
        if deleted:
            row[GithubIssuesTable.deleted.name] = deleted

        table_name = GithubIssuesTable.gen_table_name(connection_id)
        sql = f"SELECT _pk FROM `{table_name}` WHERE issue_id={issue_id}"
        issues = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
        if not issues:
            return

        _pk = issues[0].get('_pk')
        update_row = {
            "pk": _pk,
            "row": row
        }
        self.seadb_api.update_rows(self.base_id, table_name, [update_row])

    def get_github_issues_count(self, connection_id):
        table_name = GithubIssuesTable.gen_table_name(connection_id)
        sql = f"SELECT COUNT(*) as count FROM `{table_name}`"
        response = self.seadb_api.query_rows(self.base_id, sql)
        issues_count = 0
        if response and 'results' in response and response['results']:
            issues_count = response['results'][0].get('count', 0)
        return issues_count

    def save_or_update_issue_to_db(self, connection_id, issue):
        label_names = [label['name'] for label in issue.get('labels', []) if 'name' in label]
        assignees_list = [assignee['login'] for assignee in issue.get('assignees', []) if 'login' in assignee]
        issue_type = issue.get('type', None)
        if issue_type:
            type_name = issue_type['name']
        else:
            type_name = None

        issue_id = issue['id']
        content = issue.get('body')
        state_reason = issue.get('state_reason')

        if content and len(content) > MAX_CONTENT_LENGTH:
            content = content[:MAX_CONTENT_LENGTH]

        insert_row = {
            GithubIssuesTable.issue_id.name: issue['id'],
            GithubIssuesTable.issue_number.name: issue['number'],
            GithubIssuesTable.title.name: issue.get('title'),
            GithubIssuesTable.author.name: issue.get('user', {}).get('login', ''),
            GithubIssuesTable.url.name: issue.get('html_url', ''),
            GithubIssuesTable.content.name: content,
            GithubIssuesTable.state.name: issue.get('state'),
            GithubIssuesTable.assignees.name: json.dumps(assignees_list),
            GithubIssuesTable.created_time.name: time_to_utc_time(issue.get('created_at')).isoformat(),
            GithubIssuesTable.modified_time.name: time_to_utc_time(issue.get('updated_at')).isoformat(),
            GithubIssuesTable.sync_time.name: datetime.datetime.now(datetime.UTC).isoformat(),
            GithubIssuesTable.closed_time.name: time_to_utc_time(issue.get('closed_at')).isoformat() if issue.get('closed_at') else None,
            GithubIssuesTable.comments_count.name: issue.get('comments'),
            GithubIssuesTable.deleted.name: False,
        }

        if state_reason:
            insert_row[GithubIssuesTable.state_reason.name] = state_reason

        if label_names:
            insert_row[GithubIssuesTable.labels.name] = label_names

        if type_name:
            insert_row[GithubIssuesTable.issue_type.name] = type_name

        table_name = GithubIssuesTable.gen_table_name(connection_id)
        sql = f"SELECT * FROM `{table_name}` WHERE issue_id={issue_id}"
        issues = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
        if issues:
            _pk = issues[0].get('_pk')
            update_row = {
                "pk":_pk,
                "row":insert_row,
            }
            self.seadb_api.update_rows(self.base_id, table_name, [update_row])
        else:
            self.seadb_api.insert_rows(self.base_id, table_name, [insert_row])

    def save_or_update_issue_comment_to_db(self, connection_id, issue_id, comment):
        comment_id = comment['id']
        issue_id = int(issue_id)
        content = comment.get('body', '')
        if content and len(content) > MAX_CONTENT_LENGTH:
            content = content[:MAX_CONTENT_LENGTH]

        insert_row = {
            GithubIssueCommentsTable.comment_id.name: comment_id,
            GithubIssueCommentsTable.issue_id.name: issue_id,
            GithubIssueCommentsTable.author.name: comment.get('user', {}).get('login', ''),
            GithubIssueCommentsTable.content.name: content,
            GithubIssueCommentsTable.created_time.name: time_to_utc_time(comment.get('created_at')).isoformat(),
            GithubIssueCommentsTable.modified_time.name: time_to_utc_time(comment.get('updated_at')).isoformat(),
        }

        table_name = GithubIssueCommentsTable.gen_table_name(connection_id)
        sql = f"SELECT * FROM `{table_name}` WHERE comment_id={comment_id}"
        comments = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
        if comments:
            _pk = comments[0].get('_pk')
            update_row = [
                {
                "pk":_pk,
                "row":insert_row,
                }
            ]
            self.seadb_api.update_rows(self.base_id, table_name, update_row)
        else:
            self.seadb_api.insert_rows(self.base_id, table_name, [insert_row])

    def add_or_update_issue_label_column_options(self, connection_id, issues_labels):
        table_name = GithubIssuesTable.gen_table_name(connection_id)
        metadata = self.seadb_api.get_base_metadata(self.base_id)
        table_info = {}
        for table in metadata.get('tables', []):
            if table['name'] == table_name:
                table_info = table
                break
        table_id = table_info.get('id')
        columns = table_info.get("columns", [])
        label_column = {}
        for column in columns:
            if column.get('name') == GithubIssuesTable.labels.name:
                label_column = column
                break

        for issues_label in issues_labels:
            if label_column['data'] is not None:
                option_names = {opt['name'] for opt in label_column['data']['options']}
            else:
                option_names = None
            if option_names is None or issues_label.get('name') not in option_names:
                column_key = label_column.get('key')
                column_option = {
                    'table_id': table_id,
                    'column_key': column_key,
                    'option_name': issues_label.get('name'),
                    'option_data': {
                        'color': '#' + f"{issues_label.get('color')}",
                    },
                }
                self.seadb_api.add_column_option(self.base_id, column_option)

    def get_issue_by_issue_id(self, issue_id, connection_id):
        table_name = GithubIssuesTable.gen_table_name(connection_id)
        sql = f"SELECT * FROM `{table_name}` WHERE `issue_id` = {issue_id}"
        response = self.seadb_api.query_rows(self.base_id, sql)
        if response and 'results' in response and response['results']:
            return response['results'][0]
        return {}

    # *** github_issue index function ***
    def delete_issues_from_db(self, connection_id, need_delete_issue_pks, need_deleted_comment_issue_ids):
        table_name = GithubIssuesTable.gen_table_name(connection_id)
        self.seadb_api.delete_rows(self.base_id, table_name, need_delete_issue_pks)

        comments_table_name = GithubIssueCommentsTable.gen_table_name(connection_id)
        for issue_id in need_deleted_comment_issue_ids:
            delete_sql = f"DELETE FROM `{comments_table_name}` WHERE issue_id = {issue_id}"
            self.seadb_api.query_rows(self.base_id, delete_sql)

    def delete_comments_from_db(self, connection_id, issue_id):
        comments_table_name = GithubIssueCommentsTable.gen_table_name(connection_id)
        sql = f"DELETE FROM `{comments_table_name}` WHERE issue_id = {issue_id} AND deleted = True"
        self.seadb_api.query_rows(self.base_id, sql)

    def get_issues_by_page(self, connection_id, last_indexed_at, page_size, offset):
        issues_table_name = GithubIssuesTable.gen_table_name(connection_id)
        sql = f"SELECT * FROM `{issues_table_name}` WHERE sync_time >= '{last_indexed_at}' LIMIT {page_size} OFFSET {offset}"
        res = self.seadb_api.query_rows(self.base_id, sql)
        issues = res.get('results', [])
        return issues

    def get_issue_comments_by_page(self, connection_id, issue_id, page_size, offset):
        comments_table_name = GithubIssueCommentsTable.gen_table_name(connection_id)
        sql_comments = f"SELECT * FROM `{comments_table_name}` WHERE issue_id = {issue_id} LIMIT {page_size} OFFSET {offset}"
        comments = self.seadb_api.query_rows(self.base_id, sql_comments).get('results', [])
        return comments

    def get_need_ai_issues_by_page(self, connection_id, page_size):
        issues_table_name = GithubIssuesTable.gen_table_name(connection_id)
        sql = f"""
            SELECT * FROM `{issues_table_name}`
            WHERE ai_processed_time IS NULL
            OR ai_processed_time < sync_time
            ORDER BY issue_id
            LIMIT {page_size}
        """
        res = self.seadb_api.query_rows(self.base_id, sql)
        issues = res.get('results', [])
        return issues
