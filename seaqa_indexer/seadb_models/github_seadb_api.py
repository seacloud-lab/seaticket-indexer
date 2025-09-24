from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.seadb_models.models import GithubIssuesTable, GithubIssueCommentsTable

class GitHubSeaDBAPI:
    def __init__(self, base_id, username='', timeout=30):
        self.base_id = base_id
        self.seadb_api = SeaDBAPI(username=username, timeout=timeout)

    def _get_issues_table_name(self, connection_id):
        return f"{connection_id}_github_issues"

    def _get_issue_comments_table_name(self, connection_id):
        return f"{connection_id}_issue_comments"

    def save_issues_to_db(self, connection_id, insert_rows):
        insert_rows_data = [insert_rows]
        table_name = self._get_issues_table_name(connection_id)
        self.seadb_api.insert_rows(self.base_id, table_name, insert_rows_data)

    def update_issues_to_db(self, connection_id, update_rows):
        table_name = self._get_issues_table_name(connection_id)
        self.seadb_api.update_rows(self.base_id, table_name, update_rows)

    def save_issue_comments_to_db(self, connection_id, insert_rows):
        insert_rows_data = [insert_rows]
        table_name = self._get_issue_comments_table_name(connection_id)
        self.seadb_api.insert_rows(self.base_id, table_name, insert_rows_data)

    def update_issue_comments_to_db(self, connection_id, update_rows):
        table_name = self._get_issue_comments_table_name(connection_id)
        self.seadb_api.update_rows(self.base_id, table_name, update_rows)

    def query_issue(self, connection_id, issue_id):
        table_name = self._get_issues_table_name(connection_id)
        sql = f"SELECT * FROM `{table_name}` WHERE issue_id={issue_id}"
        return self.seadb_api.query_rows(self.base_id, sql).get('results', [])

    def query_issue_comment(self, connection_id, comment_id):
        table_name = self._get_issue_comments_table_name(connection_id)
        query_sql = f"SELECT * FROM `{table_name}` WHERE comment_id={comment_id}"
        return self.seadb_api.query_rows(self.base_id, query_sql).get('results', [])

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
                        GithubIssueCommentsTable.deleted.name: True,
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
                    GithubIssueCommentsTable.deleted.name: True,
                }
            }
        ]
        self.seadb_api.update_rows(self.base_id, table_name, update_rows)

    def update_issue_record(self, connection_id, issue_id, row):
        issue = self.query_issue(connection_id, issue_id)
        _pk = issue[0].get('_pk')
        table_name = self._get_issues_table_name(connection_id)
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
