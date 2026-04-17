import json
import datetime
import logging
from urllib.parse import urlparse

from sqlalchemy import table

from seaqa_indexer.handlers.base_handler import ConnectionHandler
from seaqa_indexer.seadb_models.models import GitHubPullRequestsTable, GitHubPullRequestCommentsTabel
from seaqa_indexer.utils.constants import ConnectionType
from seaqa_indexer.utils.github_issues_api import GitHubAPI, GitHubAppNotInstalled
from seaqa_indexer.seadb_models.github_seadb_api import GitHubSeaDBAPI
from seaqa_indexer.utils import decrypt_config, need_update_connection_last_sync_count, time_to_utc_time
from seaqa_indexer.index.utils import gen_index_name_by_type
from seaqa_indexer.utils.constants import IndexStatus
from seaqa_indexer.index.index_store.github_pull_requests_index import GitHubPullRequestsIndex
from seaqa_indexer.ai_processor.utils import generate_summary

logger = logging.getLogger(__name__)


class GithubPullRequestHandler(ConnectionHandler):
    """Handler for GITHUB_PR connection type."""

    connection_type = ConnectionType.GITHUB_PR.value
    BATCH_SIZE = 50

    def gen_table_name(self, connection_id):
        return GitHubPullRequestsTable.gen_table_name(connection_id)

    def gen_comments_table_name(self, connection_id):
        return GitHubPullRequestCommentsTabel.gen_table_name(connection_id)

    def get_search_param(self, seasearch_api, index_name, keyword, start, size, time_from, time_to):
        github_pr_index = GitHubPullRequestsIndex(seasearch_api)
        return github_pr_index.get_search_param(index_name, keyword, start, size, time_from, time_to)

    def delete_index_by_connection_id(self, seasearch_api, connection_id):
        index_name = gen_index_name_by_type(connection_id, self.connection_type)
        github_pr_index = GitHubPullRequestsIndex(seasearch_api)
        github_pr_index.delete_index_by_index_name(index_name)

        # summary vector index
        self.delete_summary_vector_index_by_connection_id(seasearch_api, connection_id)

    def format_vector_search_result(self, record, connection, result):
        connection_config = json.loads(connection.config)
        server_url = connection_config.get('repository')
        url_parsed = urlparse(server_url)
        base_url = url_parsed.scheme + "://" + url_parsed.netloc
        parts = url_parsed.path.strip("/").split("/")
        repo_owner, repo_name = parts[0], parts[1]
        url = f'{base_url}/{repo_owner}/{repo_name}/pull/' + str(record.get('pr_number', ''))

        return {
            'title': record.get('title', ''),
            'pr_number': record.get('pr_number', ''),
            'state': record.get('state', ''),
            'labels': record.get('labels'),
            'repo_name': repo_name,
            'repo_owner': repo_owner,
            'url': url,
            'merged': record.get('merged', False),
        }

    def process_ai(self, seadb_api, storage, connection_id, project_uuid):
        page_size = 100
        updates = []
        table_name = self.gen_table_name(connection_id)
        github_seadb_api = GitHubSeaDBAPI(project_uuid)

        ai_processing_count = 0
        while True:
            rows = github_seadb_api.get_need_ai_pull_requests_by_page(connection_id, page_size)
            if not rows:
                break
            for row in rows:
                pk = row.get('_pk')
                title = row.get('title', '')
                content = row.get('content', '')

                full_content = f"Title: {title}\n\nBody: {content}\n"
                summary, vector = generate_summary(full_content, ConnectionType.GITHUB_PR.value, project_uuid)
                updates.append({
                    "pk": pk,
                    "row": {
                        "ai_summary": summary,
                        "ai_summary_vector": vector,
                        "ai_processed_time": datetime.datetime.now(datetime.UTC).isoformat()
                    }
                })
                ai_processing_count += 1
                if len(updates) >= self.BATCH_SIZE:
                    seadb_api.update_rows(project_uuid, table_name, updates)
                    updates = []
            if updates:
                seadb_api.update_rows(project_uuid, table_name, updates)
        return ai_processing_count

    def update_index(self, project_connection, seasearch_api, seaqa_db, since):
        connection_id = project_connection.id
        config = decrypt_config(json.loads(project_connection.config))
        connection_type = project_connection.type
        server_url = config.get('repository')
        path = urlparse(server_url).path
        parts = path.strip("/").split("/")
        repo_owner, repo_name = parts[0], parts[1]
        connection_status = json.loads(project_connection.status)
        project_uuid = project_connection.project_uuid
        github_seadb_api = GitHubSeaDBAPI(project_uuid)
        github_pr_index = GitHubPullRequestsIndex(seasearch_api)

        connection_status['last_index_status'] = IndexStatus.INDEXING
        seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))

        pr_index_name = gen_index_name_by_type(connection_id, connection_type)
        github_pr_index.create_index_if_missing(pr_index_name)

        index_count = github_pr_index.update(pr_index_name, connection_id, repo_owner, repo_name,
                                             connection_status, seaqa_db, github_seadb_api, since)
        indexed_at = datetime.datetime.now(datetime.UTC)
        connection_status['last_indexed_count'] = index_count
        connection_status['last_index_status'] = IndexStatus.COMPLETED
        seaqa_db.update_connection(connection_id, indexed_at, json.dumps(connection_status))

    def sync(self, connection_id, project_uuid, config, old_last_sync_time,
             connection_status, seaqa_db, sync_logger):
        server_url = config.get('repository')
        installation_id = config.get('installation_id')

        try:
            path = urlparse(server_url).path
            parts = path.strip("/").split("/")
            repo_owner, repo_name = parts[0], parts[1]
        except Exception:
            sync_logger.log("Github repository is invalid")
            return None, None, None, None

        since = old_last_sync_time.isoformat() + "Z" if old_last_sync_time else None
        try:
            github_api = GitHubAPI(installation_id)
        except GitHubAppNotInstalled:
            sync_logger.log("Github app installation_id is incorrect")
            return None, None, None, None

        github_seadb_api = GitHubSeaDBAPI(project_uuid)
        sync_count, total_count = self._sync_github_pr(
            connection_id, since, connection_status, github_api,
            seaqa_db, github_seadb_api, repo_owner, repo_name
        )
        return sync_count, total_count, False, config

    def clean_deleted_records(self, seadb_api, project_uuid, connection_id, cutoff_time):
        page_size = 1000
        table_name = self.gen_table_name(connection_id)
        comments_table_name = self.gen_comments_table_name(connection_id)
        total_deleted = 0

        while True:
            sql = f"""
                SELECT _pk, pr_id FROM `{table_name}`
                WHERE deleted = true AND sync_time < '{cutoff_time}'
                LIMIT {page_size}
            """
            result = seadb_api.query_rows(project_uuid, sql)
            rows = result.get('results', [])
            if not rows:
                break

            deleted_pks = [row.get('_pk') for row in rows]
            deleted_pr_ids = [row.get('pr_id') for row in rows]

            seadb_api.delete_rows(project_uuid, table_name, deleted_pks)
            for pr_id in deleted_pr_ids:
                delete_sql = f"DELETE FROM `{comments_table_name}` WHERE pr_id = {pr_id}"
                seadb_api.query_rows(project_uuid, delete_sql)

            total_deleted += len(deleted_pks)

            if len(rows) < page_size:
                break

        return total_deleted

    def clean_connection_data(self, storage, seadb_api, project_uuid,
                              connection_id, tables):
        if tables:
            prs_table_name = self.gen_table_name(connection_id)
            comments_table_name = self.gen_comments_table_name(connection_id)
            for table in tables:
                if table['name'] in (prs_table_name, comments_table_name):
                    seadb_api.delete_table(project_uuid, table['id'])

    def _sync_github_pr(self, connection_id, since, connection_status, github_api,
                        seaqa_db, github_seadb_api, repo_owner, repo_name):
        page = 1
        per_page = 100
        sync_count = 0
        added_count = 0
        updated_count = 0
        _status_last_update_time = datetime.datetime.now()
        _status_last_update_count = 0

        table_name = GitHubPullRequestsTable.gen_table_name(connection_id)
        table_metadata = github_seadb_api.get_table_metadata(table_name)
        pr_labels = github_api.get_all_issue_labels(repo_owner, repo_name, per_page=per_page)
        github_seadb_api.add_or_update_issue_label_column_options(pr_labels, table_metadata)

        while True:
            prs = github_api.list_pull_requests(repo_owner, repo_name, since=since, state='all',
                                                per_page=per_page, page=page)
            if not prs:
                break

            for pr in prs:
                pr_id = pr['id']
                existed_pr = github_seadb_api.get_pull_request_by_pr_id(pr_id, connection_id)
                if existed_pr:
                    if time_to_utc_time(pr.get('updated_at')) <= time_to_utc_time(str(existed_pr.get('modified_time'))):
                        logger.debug('Pull request %s is up to date, skipping sync.', pr_id)
                        continue

                sync_count += 1
                if existed_pr:
                    updated_count += 1
                else:
                    added_count += 1

                github_seadb_api.save_or_update_pull_request_to_db(connection_id, pr, existed_pr)

                should, new_time, new_count = need_update_connection_last_sync_count(
                    sync_count, _status_last_update_time, _status_last_update_count
                )
                if should:
                    connection_status['last_sync_count'] = sync_count
                    seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
                    _status_last_update_time = new_time
                    _status_last_update_count = new_count

                comments_page = 1
                all_comments = []
                while True:
                    comments = github_api.get_pull_request_comments(
                        repo_owner, repo_name, pr['number'], per_page=per_page, page=comments_page
                    )
                    all_comments += comments
                    comments_page += 1
                    if not comments or len(comments) < per_page:
                        break

                github_seadb_api.save_or_update_pull_request_comments(connection_id, pr_id, all_comments)

            if len(prs) < per_page:
                break
            page += 1

        connection_status['last_sync_changes'] = {
            'added': added_count,
            'deleted': 0,
            'updated': updated_count,
        }
        total_count = github_seadb_api.get_github_pull_requests_count(connection_id)
        return sync_count, total_count
