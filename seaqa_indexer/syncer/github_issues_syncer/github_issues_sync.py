import logging
import json

from sqlalchemy.sql import text

from seaqa_indexer.seadb_models.github_seadb_api import GitHubSeaDBAPI
from seaqa_indexer.utils import need_update_connection_status
from seaqa_indexer.utils.github_issues_api import GitHubAPI
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.utils.seaqa_db import SeaqaDB

logger = logging.getLogger(__name__)


class GithubIssuesSync:
    def __init__(self,project_uuid, db_session_class):
        self.db_session_class = db_session_class
        self.seadb_api = SeaDBAPI()
        self.seaqa_db = SeaqaDB(db_session_class)
        self.github_seadb_api = GitHubSeaDBAPI(project_uuid)

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
                        create_select = {
                            'table_id': table_id,
                            'column_key': column_key,
                            'option_name': issues_label.get('name'),
                        }
                        try:
                            self.seadb_api.add_column_option(project_uuid, create_select)
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
                self.github_seadb_api.save_or_update_issues_to_db(connection_id, issue, is_webhook=False)
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
                        self.github_seadb_api.save_or_update_issue_comments_to_db(connection_id, issue['id'], comment, is_webhook=False)

            if len(issues) < per_page:
                return sync_count
            page += 1
