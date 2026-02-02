import json
import datetime
import logging
from urllib.parse import urlparse

from seaqa_indexer.handlers.base_handler import ConnectionHandler
from seaqa_indexer.seadb_models.models import GithubIssuesTable, GithubIssueCommentsTable
from seaqa_indexer.utils.constants import ConnectionType
from seaqa_indexer.handlers.utils import need_update_connection_last_sync_count, time_to_utc_time
from seaqa_indexer.utils.github_issues_api import GitHubAPI
from seaqa_indexer.seadb_models.github_seadb_api import GitHubSeaDBAPI

logger = logging.getLogger(__name__)
class GithubIssueHandler(ConnectionHandler):
    """Handler for GITHUB_ISSUE connection type."""
    
    connection_type = ConnectionType.GITHUB_ISSUE.value

    def gen_table_name(self, connection_id):
        return GithubIssuesTable.gen_table_name(connection_id)

    def gen_comments_table_name(self, connection_id):
        return GithubIssueCommentsTable.gen_table_name(connection_id)
    
    def get_index_instance(self, seasearch_api):
        from seaqa_indexer.index.index_store.github_issues_index import GitHubIssuesIndex
        return GitHubIssuesIndex(seasearch_api)
    
    def get_search_param(self, index_instance, index_name, keyword, start, size, time_from, time_to):
        return index_instance.get_search_param(index_name, keyword, start, size, time_from, time_to)
    
    def delete_index(self, index_manager, connection_id):
        from seaqa_indexer.index.utils import gen_index_name_by_type
        index_name = gen_index_name_by_type(connection_id, self.connection_type)
        index_manager.github_issues_index.delete_index_by_index_name(index_name)
    
    def format_vector_search_result(self, record, connection, result):
        connection_config = json.loads(connection.config)
        server_url = connection_config.get('repository')
        url_parsed = urlparse(server_url)
        base_url = url_parsed.scheme + "://" + url_parsed.netloc
        parts = url_parsed.path.strip("/").split("/")
        repo_owner, repo_name = parts[0], parts[1]
        url = f'{base_url}/{repo_owner}/{repo_name}/issues/' + str(record.get('issue_number', ''))
        
        return {
            'title': record.get('title', ''),
            'issue_number': record.get('issue_number', ''),
            'state': record.get('state', ''),
            'labels': record.get('labels'),
            'repo_name': repo_name,
            'repo_owner': repo_owner,
            'url': url,
        }
    
    def process_ai(self, seadb_api, storage, connection_id, project_uuid):
        from seaqa_indexer.ai_processor.ai_processor import process_github_issue_ai
        return process_github_issue_ai(seadb_api, connection_id, project_uuid)
    
    def update_index(self, connection, seasearch_api, seaqa_db):
        from seaqa_indexer.index.index_utils import update_github_issues_index
        update_github_issues_index(connection, seasearch_api, seaqa_db)
    
    def sync(self, connection_id, project_uuid, config, old_last_sync_time, 
             connection_status, seaqa_db, sync_logger):        
        server_url = config.get('repository')
        token = config.get('access_token')
        
        try:
            path = urlparse(server_url).path
            parts = path.strip("/").split("/")
            repo_owner, repo_name = parts[0], parts[1]
        except Exception as e:
            sync_logger.log(f"Github repository is invalid")
            return None, None
        
        since = old_last_sync_time.isoformat() + "Z" if old_last_sync_time else None
        github_api = GitHubAPI(token, repo_owner, repo_name)
        github_seadb_api = GitHubSeaDBAPI(project_uuid)
        
        sync_count, total_count = self._sync_github_issue(connection_id, since, connection_status, 
                                github_api, seaqa_db, github_seadb_api)
        return sync_count, total_count, False, config
    
    def clean_deleted_records(self, seadb_api, project_uuid, connection_id, cutoff_time):
        """Clean deleted github issue records and their comments."""
        page_size = 1000
        table_name = self.gen_table_name(connection_id)
        comments_table_name = self.gen_comments_table_name(connection_id)
        total_deleted = 0

        while True:
            sql = f"""
                SELECT _pk, issue_id FROM `{table_name}` 
                WHERE deleted = true AND sync_time < '{cutoff_time}'
                LIMIT {page_size}
            """
            result = seadb_api.query_rows(project_uuid, sql)
            rows = result.get('results', [])
            if not rows:
                break

            deleted_pks = [row.get('_pk') for row in rows]
            deleted_issue_ids = [row.get('issue_id') for row in rows]

            seadb_api.delete_rows(project_uuid, table_name, deleted_pks)
            for issue_id in deleted_issue_ids:
                delete_sql = f"DELETE FROM `{comments_table_name}` WHERE issue_id = {issue_id}"
                seadb_api.query_rows(project_uuid, delete_sql)

            total_deleted += len(deleted_pks)

            if len(rows) < page_size:
                break

        return total_deleted
    
    def clean_connection_data(self, storage, seadb_api, project_uuid, 
                              connection_id, tables):
        """Clean github issue connection data including issues and comments tables."""
        if tables:
            issues_table_name = self.gen_table_name(connection_id)
            comments_table_name = self.gen_comments_table_name(connection_id)
            for table in tables:
                if table['name'] in (issues_table_name, comments_table_name):
                    seadb_api.delete_table(project_uuid, table['id'])
    
    def add_sync_task(self, app, connection_id):
        app.sync_manager.add_sync_task(connection_id)
    
    def add_recovering_sync_task(self, mq, connection_id):
        msg_content = {
            'connection_id': connection_id,
        }
        mq.lpush('sync_task', json.dumps(msg_content))

    def _sync_github_issue(self, connection_id, since, connection_status, github_api, seaqa_db, github_seadb_api):
        page = 1
        per_page = 100  # max 100
        sync_count = 0
        _status_last_update_time = datetime.datetime.now()
        _status_last_update_count = 0

        issues_labels = github_api.get_issues_labels(page, per_page)
        github_seadb_api.add_or_update_issue_label_column_options(connection_id, issues_labels)

        while True:
            issues = github_api.list_issues(since=since, state='all', per_page=per_page, page=page)
            if not issues:
                break

            for issue in issues:
                if 'pull_request' in issue:
                    continue

                # Check if issue already exists in DB
                issue_id = issue['id']
                existed_issue = github_seadb_api.get_issue_by_issue_id(issue_id, connection_id)
                if existed_issue:
                    if time_to_utc_time(issue.get('updated_at')) <= time_to_utc_time(str(existed_issue.get('modified_time'))):
                        logger.debug(f'Issue {issue_id} is up to date, skipping sync.')
                        continue

                sync_count += 1
                github_seadb_api.save_or_update_issue_to_db(connection_id, issue, existed_issue)
                should, new_time, new_count = need_update_connection_last_sync_count(
                    sync_count, _status_last_update_time, _status_last_update_count
                )
                if should:
                    connection_status['last_sync_count'] = sync_count
                    seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
                    _status_last_update_time = new_time
                    _status_last_update_count = new_count

                # sync issue comments
                comments_page = 1
                all_comments = []
                while True:
                    comments = github_api.get_issue_comments(issue['number'], per_page=per_page, page=comments_page)
                    all_comments += comments
                    comments_page += 1
                    if not comments or len(comments) < per_page:
                        break

                github_seadb_api.save_or_update_issue_comments(connection_id, issue_id, all_comments)

            if len(issues) < per_page:
                break
            page += 1

        total_count = github_seadb_api.get_github_issues_count(connection_id)
        return sync_count, total_count
