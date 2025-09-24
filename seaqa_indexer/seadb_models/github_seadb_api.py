import json


from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.seadb_models.models import GITHUB_ISSUES_TABLE, GITHUB_ISSUES_COMMENTS_TABLE


MAX_BODY_LENGTH = 10000


class GitHubSeaDBAPI:
    def __init__(self, base_id, username='', timeout=30):
        self.base_id = base_id
        self.seadb_api = SeaDBAPI(username=username, timeout=timeout)

    def _get_issues_table_name(self, connection_id):
        return f"{connection_id}_github_issues"

    def _get_issue_comments_table_name(self, connection_id):
        return f"{connection_id}_issue_comments"

    def save_or_update_issues_to_db(self, connection_id, issue, is_webhook):
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
        created_at = issue.get('created_at') if 'created_at' in issue else None
        updated_at = issue.get('updated_at') if issue.get('updated_at') else None
        closed_at = issue.get('closed_at') if issue.get('closed_at') else None
        comments_count = issue.get('comments')
        deleted = False

        if body and len(body) > MAX_BODY_LENGTH:
            body = body[:MAX_BODY_LENGTH]

        insert_rows = [
            {
                GITHUB_ISSUES_TABLE.columns.issue_id.name:issue_id,
                GITHUB_ISSUES_TABLE.columns.issue_number.name: issue_number,
                GITHUB_ISSUES_TABLE.columns.title.name: title,
                GITHUB_ISSUES_TABLE.columns.author.name: author,
                GITHUB_ISSUES_TABLE.columns.url.name: url,
                GITHUB_ISSUES_TABLE.columns.body.name: body,
                GITHUB_ISSUES_TABLE.columns.state.name: state,
                GITHUB_ISSUES_TABLE.columns.assignees.name: assignees,
                GITHUB_ISSUES_TABLE.columns.created_at.name: created_at,
                GITHUB_ISSUES_TABLE.columns.updated_at.name: updated_at,
                GITHUB_ISSUES_TABLE.columns.closed_at.name: closed_at,
                GITHUB_ISSUES_TABLE.columns.comments_count.name: comments_count,
                GITHUB_ISSUES_TABLE.columns.deleted.name: deleted,
            }
        ]

        if state_reason is not None:
            insert_rows[0][GITHUB_ISSUES_TABLE.columns.state_reason.name] = state_reason

        if label_names is not None:
            insert_rows[0][GITHUB_ISSUES_TABLE.columns.labels.name] = label_names

        if type_name is not None:
            insert_rows[0][GITHUB_ISSUES_TABLE.columns.issue_type.name] = type_name

        table_name = self._get_issues_table_name(connection_id)
        if is_webhook:
            query_sql = f"SELECT * FROM `{table_name}` WHERE issue_id={issue_id}"
            res = self.seadb_api.query_rows(self.base_id, query_sql).get('results', [])
            if res:
                _pk = res[0].get('_pk')
                update_rows = [
                    {
                    "pk":_pk,
                    "row":insert_rows[0],
                    }
                ]
                self.seadb_api.update_rows(self.base_id, table_name, update_rows)
            else:
                self.seadb_api.insert_rows(self.base_id, table_name, insert_rows)
        else:
            self.seadb_api.insert_rows(self.base_id, table_name, insert_rows)

    def save_or_update_issue_comments_to_db(self, connection_id, issue_id, comment, is_webhook):
        table_name = self._get_issue_comments_table_name(connection_id)
        created_at = comment.get('created_at') if 'created_at' in comment else None
        updated_at = comment.get('updated_at') if comment.get('updated_at') else None
        comment_id = comment['id']
        issue_id = int(issue_id)
        author = comment.get('user', {}).get('login', '')
        body = comment.get('body', '')
        if body and len(body) > MAX_BODY_LENGTH:
            body = body[:MAX_BODY_LENGTH]

        insert_rows = [
            {
                GITHUB_ISSUES_COMMENTS_TABLE.columns.comment_id.name: comment_id,
                GITHUB_ISSUES_COMMENTS_TABLE.columns.issue_id.name: issue_id,
                GITHUB_ISSUES_COMMENTS_TABLE.columns.author.name: author,
                GITHUB_ISSUES_COMMENTS_TABLE.columns.body.name: body,
                GITHUB_ISSUES_COMMENTS_TABLE.columns.created_at.name: created_at,
                GITHUB_ISSUES_COMMENTS_TABLE.columns.updated_at.name: updated_at,
            }
        ]

        if is_webhook:
            query_sql = f"SELECT * FROM `{table_name}` WHERE issue_id={issue_id}"
            res = self.seadb_api.query_rows(self.base_id, query_sql).get('results', [])
            if res:
                _pk = res[0].get('_pk')
                update_rows = [
                    {
                    "pk":_pk,
                    "row":insert_rows[0],
                    }
                ]
                self.seadb_api.update_rows(self.base_id, table_name, update_rows)
            else:
                self.seadb_api.insert_rows(self.base_id, table_name, insert_rows)
        else:
            self.seadb_api.insert_rows(self.base_id,table_name, insert_rows)

    def tag_comment_deleted(self, connection_id, comment_id):
        table_name = self._get_issue_comments_table_name(connection_id)
        sql = f"SELECT * FROM `{table_name}` WHERE comment_id = {comment_id}"
        issue = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
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
        self.seadb_api.update_rows(self.base_id, table_name, update_rows)

    def tag_issue_deleted(self, connection_id, issue_id):
        table_name = self._get_issues_table_name(connection_id)
        sql = f"SELECT * FROM `{table_name}` WHERE issue_id = {issue_id}"
        issue = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
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
        self.seadb_api.update_rows(self.base_id, table_name, update_rows)

    def update_issue_record(self, connection_id, issue_id, updated_at, comments_count=None):
        table_name = self._get_issues_table_name(connection_id)
        sql = f"SELECT * FROM `{table_name}` WHERE issue_id = {issue_id}"
        issue = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
        _pk = issue[0].get('_pk')

        row = {
            GITHUB_ISSUES_TABLE.columns.updated_at.name: updated_at,
        }

        if comments_count is not None:
            comments_count = int(comments_count)
            row[GITHUB_ISSUES_TABLE.columns.comments_count.name] = comments_count

        update_rows = [
            {
                "pk": _pk,
                "row":row
            }
        ]
        self.seadb_api.update_rows(self.base_id, table_name, update_rows)

    def get_github_issues_count(self, connection_id):
        table_name = self._get_issues_table_name(connection_id)
        sql = f"SELECT count(*) FROM `{table_name}`"
        issues_count = self.seadb_api.query_rows(self.base_id, sql).get('results', [])[0]
        return issues_count

    def delete_issues_from_db(self, connection_id):
        table_name = self._get_issues_table_name(connection_id)
        sql = f"SELECT * FROM `{table_name}` WHERE deleted = True"
        need_delete = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
        need_delete_rows = []
        if need_delete:
            for issue in need_delete:
                need_delete_rows.append(issue.get('_pk'))
        self.seadb_api.delete_rows(self.base_id, table_name, need_delete_rows)

    def delete_comments_from_db(self, connection_id, issue_id):
        table_name = self._get_issue_comments_table_name(connection_id)
        sql = f"SELECT * FROM `{table_name}` WHERE issue_id = {issue_id} AND deleted = True"
        need_delete = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
        need_delete_rows = []
        if need_delete:
            for comment in need_delete:
                need_delete_rows.append(comment.get('_pk'))
        self.seadb_api.delete_rows(self.base_id, table_name, need_delete_rows)

    def delete_all_data_by_connection_id(self, connection_id):
        issues_table_name = self._get_issues_table_name(connection_id)
        sql_issues = f"SELECT * FROM `{issues_table_name}` WHERE deleted = True"
        need_delete = self.seadb_api.query_rows(self.base_id, sql_issues).get('results', [])
        need_delete_rows = []
        need_delete_issue_comments = []
        if need_delete:
            for issue in need_delete:
                need_delete_rows.append(issue.get('_pk'))
                need_delete_issue_comments.append(issue.get('issue_id'))
        self.seadb_api.delete_rows(self.base_id, issues_table_name, need_delete_rows)

        comments_table_name = self._get_issue_comments_table_name(connection_id)
        need_delete_comments_pk = []
        for issue_id in need_delete_issue_comments:
            sql_comments = f"SELECT * FROM `{comments_table_name}` WHERE issue_id = {issue_id}"
            need_delete_comments_row = self.seadb_api.query_rows(self.base_id, sql_comments).get('results', [])

            for comment in need_delete_comments_row:
                need_delete_comments_pk.append(comment.get('_pk'))
