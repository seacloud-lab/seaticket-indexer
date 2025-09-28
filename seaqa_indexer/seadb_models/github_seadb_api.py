from seaqa_indexer.utils.seadb_api import SeaDBAPI

class GitHubSeaDBAPI:
    def __init__(self, base_id, username='', timeout=30):
        self.base_id = base_id
        self.seadb_api = SeaDBAPI(username=username, timeout=timeout)

    def _get_issues_table_name(self, connection_id):
        return f"{connection_id}_github_issues"

    def _get_issue_comments_table_name(self, connection_id):
        return f"{connection_id}_issue_comments"

    def save_issue_to_db(self, connection_id, insert_row):
        insert_row_data = [insert_row]
        table_name = self._get_issues_table_name(connection_id)
        self.seadb_api.insert_rows(self.base_id, table_name, insert_row_data)

    def update_issue_to_db(self, connection_id, update_row):
        table_name = self._get_issues_table_name(connection_id)
        self.seadb_api.update_rows(self.base_id, table_name, update_row)

    def save_issue_comment_to_db(self, connection_id, insert_row):
        insert_row_data = [insert_row]
        table_name = self._get_issue_comments_table_name(connection_id)
        self.seadb_api.insert_rows(self.base_id, table_name, insert_row_data)

    def update_issue_comment_to_db(self, connection_id, update_row):
        table_name = self._get_issue_comments_table_name(connection_id)
        self.seadb_api.update_rows(self.base_id, table_name, update_row)

    def query_issue(self, connection_id, issue_id):
        table_name = self._get_issues_table_name(connection_id)
        sql = f"SELECT * FROM `{table_name}` WHERE issue_id={issue_id}"
        return self.seadb_api.query_rows(self.base_id, sql).get('results', [])

    def query_issue_comment(self, connection_id, comment_id):
        table_name = self._get_issue_comments_table_name(connection_id)
        sql = f"SELECT * FROM `{table_name}` WHERE comment_id={comment_id}"
        return self.seadb_api.query_rows(self.base_id, sql).get('results', [])

    def tag_comment_deleted(self, connection_id, comment_id):
        table_name = self._get_issue_comments_table_name(connection_id)
        sql = f"UPDATE `{table_name}` SET deleted = True WHERE comment_id = {comment_id}"
        self.seadb_api.query_rows(self.base_id, sql)

    def update_issue_record(self, connection_id, issue_id, row):
        issue = self.query_issue(connection_id, issue_id)
        _pk = issue[0].get('_pk')
        table_name = self._get_issues_table_name(connection_id)
        update_row = [
            {
                "pk": _pk,
                "row":row
            }
        ]
        self.seadb_api.update_rows(self.base_id, table_name, update_row)

    def get_github_issues_count(self, connection_id):
        table_name = self._get_issues_table_name(connection_id)
        sql = f"SELECT COUNT(*) as count FROM `{table_name}`"
        issues_result = self.seadb_api.query_rows(self.base_id, sql)
        issues_count = 0
        if issues_result and 'results' in issues_result and issues_result['results']:
            issues_count = issues_result['results'][0].get('count', 0)
        return issues_count

    def delete_issues_from_db(self, connection_id, need_delete_issue_pks, need_deleted_comment_issue_ids):
        table_name = self._get_issues_table_name(connection_id)
        self.seadb_api.delete_rows(self.base_id, table_name, need_delete_issue_pks)

        comments_table_name = self._get_issue_comments_table_name(connection_id)
        for issue_id in need_deleted_comment_issue_ids:
            delete_sql = f"DELETE FROM `{comments_table_name}` WHERE issue_id = {issue_id}"
            self.seadb_api.query_rows(self.base_id, delete_sql)

    def delete_comments_from_db(self, connection_id, issue_id):
        table_name = self._get_issue_comments_table_name(connection_id)
        sql = f"DELETE FROM `{table_name}` WHERE issue_id = {issue_id} AND deleted = True"
        self.seadb_api.query_rows(self.base_id, sql)
