import json
import datetime
import logging

from seaqa_indexer.handlers.base_handler import ConnectionHandler
from seaqa_indexer.seadb_models.models import SchemaTables
from seaqa_indexer.utils.constants import ConnectionType, IndexStatus
from seaqa_indexer.utils.linear_api import LinearAPI
from seaqa_indexer.seadb_models.linear_seadb_api import LinearSeaDBAPI
from seaqa_indexer.utils.seadb_api import OptionValueInvalidException
from seaqa_indexer.index.utils import gen_index_name_by_type
from seaqa_indexer.utils import need_update_connection_last_sync_count, time_to_utc_time
from seaqa_indexer.index.index_store.linear_index import LinearIssuesIndex

logger = logging.getLogger(__name__)


class LinearIssueHandler(ConnectionHandler):
    connection_type = ConnectionType.LINEAR.value
    BATCH_SIZE = 50

    def gen_table_name(self, connection_id):
        return SchemaTables.LINEAR_ISSUES.table_name(connection_id)

    def gen_comments_table_name(self, connection_id):
        return SchemaTables.LINEAR_ISSUE_COMMENTS.table_name(connection_id)

    def get_search_param(self, seasearch_api, index_name, keyword, start, size, time_from, time_to):
        linear_issue_index = LinearIssuesIndex(seasearch_api)
        return linear_issue_index.get_search_param(index_name, keyword, start, size, time_from, time_to)

    def delete_index_by_connection_id(self, seasearch_api, connection_id):
        index_name = gen_index_name_by_type(connection_id, self.connection_type)
        linear_issues_index = LinearIssuesIndex(seasearch_api)
        linear_issues_index.delete_index_by_index_name(index_name)

        # summary vector index
        self.delete_summary_vector_index_by_connection_id(seasearch_api, connection_id)

    def format_vector_search_result(self, record, connection, result):
        connection_config = json.loads(connection.config)
        workspace_name = connection_config.get('workspace_name')
        identifier = record.get('identifier', '')
        issue_title = record.get('issue_title', '')
        url = f"https://linear.app/{workspace_name}/issue/{identifier}/{issue_title}" if workspace_name and identifier and issue_title else ""

        return {
            'title': issue_title,
            'state': record.get('state', ''),
            'labels': record.get('labels'),
            'url': url,
        }

    def process_ai(self, seadb_api, storage, connection_id, project_uuid):
        page_size = 100
        updates = []
        table_name = self.gen_table_name(connection_id)
        linear_seadb_api = LinearSeaDBAPI(project_uuid)

        ai_processing_count = 0
        while True:
            rows = linear_seadb_api.get_need_ai_issues_by_page(connection_id, page_size)
            if not rows:
                break
            for row in rows:
                pk = row.get('_pk')
                title = row.get('title', '')
                content = row.get('content', '')

                full_content = f"Title: {title}\n\nBody: {content}\n"
                summary = self.seaqa_ai_api.generate_summary(full_content, ConnectionType.LINEAR.value, project_uuid)
                vector = self.seaqa_ai_api.generate_embedding(summary, project_uuid)
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
        connection_type = project_connection.type
        connection_status = json.loads(project_connection.status)
        project_uuid = project_connection.project_uuid
        linear_seadb_api = LinearSeaDBAPI(project_uuid)
        linear_issues_index = LinearIssuesIndex(seasearch_api)

        connection_status['last_index_status'] = IndexStatus.INDEXING
        seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))

        issues_index_name = gen_index_name_by_type(connection_id, connection_type)
        linear_issues_index.create_index_if_missing(issues_index_name)

        index_count = linear_issues_index.update(issues_index_name, connection_id, connection_status, seaqa_db, linear_seadb_api, since)
        # finish index
        indexed_at = datetime.datetime.now(datetime.UTC)
        connection_status['last_indexed_count'] = index_count
        connection_status['last_index_status'] = IndexStatus.COMPLETED
        seaqa_db.update_connection(connection_id, indexed_at, json.dumps(connection_status))

    def sync(self, connection_id, project_uuid, config, old_last_sync_time,
             connection_status, seaqa_db, sync_logger):
        oauth_info = seaqa_db.get_project_linear_oauth(project_uuid)
        access_token = oauth_info.access_token
        refresh_token = oauth_info.refresh_token
        team_id = config.get('team_id')
        include_archived = config.get('include_archived', True)
        if not access_token:
            sync_logger.log("Linear access_token is missing in project_linear_oauth")
            return None, None, None, None

        linear_api = LinearAPI(
            access_token=access_token,
            refresh_token=refresh_token
        )

        since = old_last_sync_time.isoformat() + "Z" if old_last_sync_time else None
        linear_seadb_api = LinearSeaDBAPI(project_uuid)

        is_first_sync = not old_last_sync_time
        sync_count, total_count = self._sync_linear_issue(
            connection_id, since, connection_status, linear_api, seaqa_db, linear_seadb_api,
            team_id, include_archived, is_first_sync
        )

        is_update_config = False

        return sync_count, total_count, is_update_config, config

    def clean_deleted_records(self, seadb_api, project_uuid, connection_id, cutoff_time):
        """Clean deleted linear issue records and their comments."""
        page_size = 1000
        table_name = self.gen_table_name(connection_id)
        comments_table_name = self.gen_comments_table_name(connection_id)
        total_deleted = 0

        while True:
            sql = f"""
                SELECT _pk, issue_id FROM `{table_name}` 
                WHERE deleted = true AND record_modified_time < '{cutoff_time}'
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
                issue_id_str = str(issue_id).replace("'", "''")
                delete_sql = f"DELETE FROM `{comments_table_name}` WHERE issue_id = '{issue_id_str}'"
                seadb_api.query_rows(project_uuid, delete_sql)

            total_deleted += len(deleted_pks)

            if len(rows) < page_size:
                break

        return total_deleted

    def clean_connection_data(self, storage, seadb_api, project_uuid,
                              connection_id, tables):
        """Clean linear issue connection data including issues and comments tables."""
        if tables:
            issues_table_name = self.gen_table_name(connection_id)
            comments_table_name = self.gen_comments_table_name(connection_id)
            for table in tables:
                if table['name'] in (issues_table_name, comments_table_name):
                    seadb_api.delete_table(project_uuid, table['id'])

    def sync_linear_options(self, connection_id, linear_seadb_api, linear_api, team_id=None):
            table_info = linear_seadb_api.get_table_metadata(connection_id)

            labels = linear_api.get_issue_labels()
            linear_seadb_api.add_or_update_label_column_options(labels, table_info)

            states = linear_api.get_workflow_states(team_id=team_id)
            linear_seadb_api.add_or_update_state_column_options(states, table_info)


    def _sync_linear_issue(self, connection_id, since, connection_status, linear_api, seaqa_db, linear_seadb_api, team_id, include_archived, is_first_sync):
        cursor = None
        per_page = 50
        sync_count = 0
        _status_last_update_time = datetime.datetime.now()
        _status_last_update_count = 0
        if is_first_sync:
            self.sync_linear_options(connection_id, linear_seadb_api, linear_api, team_id=team_id)

        while True:
            issues, page_info = linear_api.list_issues(
                since=since,
                after=cursor,
                first=per_page,
                team_id=team_id,
                include_archived=include_archived,
            )

            if not issues:
                break

            for issue in issues:
                # Check if issue already exists in DB
                issue_id = issue.get('id')
                existed_issue = linear_seadb_api.get_issue_by_issue_id(issue_id, connection_id)
                if existed_issue and issue.get('updatedAt'):
                    if time_to_utc_time(issue.get('updatedAt')) <= time_to_utc_time(str(existed_issue.get('modified_time'))):
                        logger.debug(f'Issue {issue_id} is up to date, skipping sync.')
                        continue

                sync_count += 1

                try:
                    linear_seadb_api.save_or_update_issue_to_db(connection_id, issue, existed_issue)
                except OptionValueInvalidException as e:
                    logger.warning('The option name may not exist; resynchronize it. exception info is: %s', e)
                    self.sync_linear_options(connection_id, linear_seadb_api, linear_api, team_id=team_id)
                    linear_seadb_api.save_or_update_issue_to_db(connection_id, issue, existed_issue)

                should, new_time, new_count = need_update_connection_last_sync_count(
                    sync_count, _status_last_update_time, _status_last_update_count
                )
                if should:
                    connection_status['last_sync_count'] = sync_count
                    seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
                    _status_last_update_time = new_time
                    _status_last_update_count = new_count

                # sync issue comments
                comments_cursor = None
                all_comments = []
                while True:
                    comments, comments_page_info = linear_api.list_issue_comments(
                        issue.get('id'),
                        after=comments_cursor,
                        first=per_page,
                    )
                    all_comments += comments
                    if not comments_page_info.get('hasNextPage'):
                        break
                    comments_cursor = comments_page_info.get('endCursor')

                linear_seadb_api.save_or_update_issue_comments(connection_id, issue_id, all_comments)

            if not page_info.get('hasNextPage'):
                break
            cursor = page_info.get('endCursor')

        total_count = linear_seadb_api.get_linear_issues_count(connection_id)
        return sync_count, total_count
