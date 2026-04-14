import json
import datetime
import logging

from seaqa_indexer.handlers.base_handler import ConnectionHandler
from seaqa_indexer.seadb_models.models import JiraIssuesTable, JiraIssueCommentsTable
from seaqa_indexer.utils.constants import ConnectionType, IndexStatus
from seaqa_indexer.seadb_models.jira_seadb_api import JiraSeaDBAPI
from seaqa_indexer.index.index_store.jira_issues_index import JiraIssuesIndex
from seaqa_indexer.utils import need_update_connection_last_sync_count, time_to_utc_time
from seaqa_indexer.index.utils import gen_index_name_by_type
from seaqa_indexer.ai_processor.utils import generate_summary
from seaqa_indexer.utils.jira_issues_api import JiraAPI


logger = logging.getLogger(__name__)


class JiraIssueHandler(ConnectionHandler):
    connection_type = ConnectionType.JIRA_ISSUE.value
    BATCH_SIZE = 50

    def gen_table_name(self, connection_id):
        return JiraIssuesTable.gen_table_name(connection_id)

    def gen_comments_table_name(self, connection_id):
        return JiraIssueCommentsTable.gen_table_name(connection_id)

    def get_search_param(self, seasearch_api, index_name, keyword, start, size, time_from, time_to):
        jira_issue_index = JiraIssuesIndex(seasearch_api)
        return jira_issue_index.get_search_param(index_name, keyword, start, size, time_from, time_to)

    def delete_index_by_connection_id(self, seasearch_api, connection_id):
        index_name = gen_index_name_by_type(connection_id, self.connection_type)
        jira_issues_index = JiraIssuesIndex(seasearch_api)
        jira_issues_index.delete_index_by_index_name(index_name)

        # summary vector index
        self.delete_summary_vector_index_by_connection_id(seasearch_api, connection_id)

    def format_vector_search_result(self, record, connection, result):
        connection_config = json.loads(connection.config)
        base_url = connection_config.get('base_url', '').rstrip('/')
        issue_key = record.get('issue_key')
        url = f'{base_url}/browse/{issue_key}' if base_url and issue_key else ''

        return {
            'title': record.get('title', ''),
            'issue_key': issue_key,
            'status': record.get('status', ''),
            'priority': record.get('priority', ''),
            'assignees': record.get('assignees', ''),
            'issue_type': record.get('issue_type', ''),
            'url': url,
        }


    def update_index(self, project_connection, seasearch_api, seaqa_db, since):
        connection_id = project_connection.id
        connection_type = project_connection.type
        connection_status = json.loads(project_connection.status)
        project_uuid = project_connection.project_uuid
        jira_seadb_api = JiraSeaDBAPI(project_uuid)
        jira_issues_index = JiraIssuesIndex(seasearch_api)

        connection_status['last_index_status'] = IndexStatus.INDEXING
        seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))

        issues_index_name = gen_index_name_by_type(connection_id, connection_type)
        jira_issues_index.create_index_if_missing(issues_index_name)

        index_count = jira_issues_index.update(issues_index_name, connection_id, connection_status, seaqa_db, jira_seadb_api, since)
        indexed_at = datetime.datetime.now(datetime.UTC)
        connection_status['last_indexed_count'] = index_count
        connection_status['last_index_status'] = IndexStatus.COMPLETED
        seaqa_db.update_connection(connection_id, indexed_at, json.dumps(connection_status))

    def sync(self, connection_id, project_uuid, config, old_last_sync_time,
             connection_status, seaqa_db, sync_logger):
        base_url = config.get('base_url', '').rstrip('/')
        user_email = config.get('user_email')
        api_token = config.get('api_token')
        space_key = config.get('space_key')

        if not base_url or not user_email or not api_token or not space_key:
            sync_logger.log('Jira configuration is invalid')
            return None, None, None, None

        jira_api = JiraAPI(base_url, user_email, api_token)
        jira_seadb_api = JiraSeaDBAPI(project_uuid)
        self._sync_jira_select_options(connection_id, jira_api, jira_seadb_api)

        sync_count, total_count = self._sync_jira_issues(
            connection_id,
            jira_api,
            space_key,
            old_last_sync_time,
            connection_status,
            seaqa_db,
            jira_seadb_api
        )
        return sync_count, total_count, False, config

    def _sync_jira_select_options(self, connection_id, jira_api, jira_seadb_api):
        table_metadata = jira_seadb_api.get_table_metadata(connection_id)
        if not table_metadata:
            return

        priorities = jira_api.get_priorities()
        jira_seadb_api.add_or_update_jira_priority_options(table_metadata, priorities)

        statuses = jira_api.get_statuses()
        jira_seadb_api.add_or_update_jira_status_options(table_metadata, statuses)

        issue_types = jira_api.get_issue_types()
        jira_seadb_api.add_or_update_jira_issue_type_options(table_metadata, issue_types)

    def _sync_jira_issues(self, connection_id, jira_api, space_key,
                          old_last_sync_time, connection_status, seaqa_db, jira_seadb_api):
        page_size = 50
        start_at = 0
        sync_count = 0
        _status_last_update_time = datetime.datetime.now()
        _status_last_update_count = 0
        table_metadata = jira_seadb_api.get_table_metadata(connection_id)

        while True:
            data = jira_api.search_issues(space_key, old_last_sync_time, start_at, page_size)
            issues = data.get('issues', [])
            total = data.get('total', 0)

            if not issues:
                break

            jira_seadb_api.add_or_update_jira_assignee_options(table_metadata, issues)

            for issue in issues:
                fields_data = issue.get('fields', {}) or {}
                issue_id = issue.get('id')

                modified_time = fields_data.get('updated')
                comments_data = (fields_data.get('comment') or {}).get('comments', [])
                existed_issue = jira_seadb_api.get_issue_by_issue_id(issue_id, connection_id)
                if existed_issue:
                    if time_to_utc_time(modified_time) <= time_to_utc_time(str(existed_issue.get('modified_time'))):
                        continue

                sync_count += 1
                jira_seadb_api.save_or_update_issue_to_db(connection_id, issue, existed_issue)

                jira_seadb_api.save_or_update_issue_comments(connection_id, issue_id, comments_data)

                should, new_time, new_count = need_update_connection_last_sync_count(
                    sync_count, _status_last_update_time, _status_last_update_count
                )
                if should:
                    connection_status['last_sync_count'] = sync_count
                    seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
                    _status_last_update_time = new_time
                    _status_last_update_count = new_count

            start_at += page_size
            if start_at >= total:
                break

        total_count = jira_seadb_api.get_jira_issues_count(connection_id)

        return sync_count, total_count

    def clean_connection_data(self, storage, seadb_api, project_uuid,
                              connection_id, tables):
        """Clean jira issue connection data including issues and comments tables."""
        if tables:
            issues_table_name = self.gen_table_name(connection_id)
            comments_table_name = self.gen_comments_table_name(connection_id)
            for table in tables:
                if table['name'] in (issues_table_name, comments_table_name):
                    seadb_api.delete_table(project_uuid, table['id'])

    def process_ai(self, seadb_api, storage, connection_id, project_uuid):
        page_size = 100
        updates = []
        table_name = self.gen_table_name(connection_id)
        jira_seadb_api = JiraSeaDBAPI(project_uuid)

        ai_processing_count = 0
        while True:
            rows = jira_seadb_api.get_need_ai_issues_by_page(connection_id, page_size)
            if not rows:
                break

            for row in rows:
                pk = row.get('_pk')
                title = row.get('title', '')
                content = row.get('content', '')
                full_content = f"Title: {title}\n\nContent: {content}\n"
                summary, vector = generate_summary(full_content, ConnectionType.JIRA_ISSUE.value, project_uuid)
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
                updates = []

        return ai_processing_count

    def clean_deleted_records(self, seadb_api, project_uuid, connection_id, cutoff_time):
        """Clean deleted jira issue records and their comments."""
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
