import datetime

from seaqa_indexer.utils import time_to_utc_time
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.seadb_models.models import SchemaTables
from seaqa_indexer.utils.jira_utils import normalize_jira_text


MAX_CONTENT_LENGTH = 10000


class JiraSeaDBAPI:
    def __init__(self, base_id):
        self.base_id = base_id
        self.seadb_api = SeaDBAPI()

    def get_table_metadata(self, connection_id):
        table_name = SchemaTables.JIRA_ISSUES.table_name(connection_id)
        metadata = self.seadb_api.get_base_metadata(self.base_id)
        for table in metadata.get('tables', []):
            if table['name'] == table_name:
                return table
        return {}

    def _get_column(self, table_metadata, column_name):
        for column in table_metadata.get('columns', []):
            if column.get('name') == column_name:
                return column
        return {}

    def _upsert_select_column_options(self, table_metadata, column_name, options):
        column = self._get_column(table_metadata, column_name)
        if not column:
            return
        table_id = table_metadata.get('id')
        column_key = column.get('key')
        existing_options = (column.get('data') or {}).get('options') or []
        existing_map = {opt.get('name'): opt for opt in existing_options if opt.get('name')}
        existing_names = set(existing_map.keys())
        new_names = set()

        for option in options:
            name = option.get('name')
            new_names.add(name)
            if not name:
                continue
            color = option.get('color')
            text_color = option.get('text_color')
            existing = existing_map.get(name)
            if not existing:
                option_payload = {
                    'table_id': table_id,
                    'column_key': column_key,
                    'option_name': name,
                }
                option_data = {}
                if color:
                    option_data['color'] = color
                if text_color:
                    option_data['text_color'] = text_color
                if option_data:
                    option_payload['option_data'] = option_data
                self.seadb_api.add_column_option(self.base_id, option_payload)
                continue

            existing_color = existing.get('color')
            existing_text_color = existing.get('text_color') or existing.get('textColor')
            update_data = {}
            if color and existing_color != color:
                update_data['color'] = color
            if text_color and existing_text_color != text_color:
                update_data['text_color'] = text_color
            if update_data:
                payload = {
                    'table_id': table_id,
                    'column_key': column_key,
                    'option_id': existing.get('id'),
                    'update_option_data': update_data,
                }
                self.seadb_api.update_column_option(self.base_id, payload)
        need_deleted_names = existing_names - new_names
        if need_deleted_names:
            need_deleted = {
                table_id: [{
                    column_key: list(need_deleted_names)
                }]
            }
            return need_deleted
        return {}

    def add_or_update_jira_priority_options(self, table_metadata, priorities):
        options = []
        for priority in priorities or []:
            name = priority.get('name')
            if not name:
                continue
            options.append({
                'name': name,
                'color': '#FFFFFF',
                'text_color': '#000000'
            })
        return self._upsert_select_column_options(table_metadata, SchemaTables.JIRA_ISSUES.column.priority.name, options)

    def add_or_update_jira_status_options(self, table_metadata, statuses):
        category_colors = {
            'new': '#42526E',
            'indeterminate': '#0052CC',
            'done': '#00875A',
        }
        options = []
        for status in statuses or []:
            name = status.get('name')
            if not name:
                continue
            category_key = (status.get('statusCategory') or {}).get('key')
            color = category_colors.get(category_key)
            option = {'name': name}
            if color:
                option['color'] = color
                option['text_color'] = '#FFFFFF'
            options.append(option)
        return self._upsert_select_column_options(table_metadata, SchemaTables.JIRA_ISSUES.column.status.name, options)

    def add_or_update_jira_issue_type_options(self, table_metadata, issue_types):
        options = []
        for issue_type in issue_types or []:
            name = issue_type.get('name')
            if not name:
                continue
            options.append({
                'name': name,
                'color': '#FFFFFF',
                'text_color': '#000000'
            })
        return self._upsert_select_column_options(table_metadata, SchemaTables.JIRA_ISSUES.column.issue_type.name, options)

    def add_or_update_jira_assignee_options(self, table_metadata, issues):
        names = set()
        for issue in issues or []:
            fields_data = issue.get('fields', {}) or {}
            assignee_name = (fields_data.get('assignee') or {}).get('displayName')
            if assignee_name:
                names.add(assignee_name)

        options = [
            {
                'name': name,
                'color': '#FFFFFF',
                'text_color': '#000000'
            }
            for name in sorted(names)
        ]
        return self._upsert_select_column_options(table_metadata, SchemaTables.JIRA_ISSUES.column.assignees.name, options)

    def get_issue_by_issue_id(self, issue_id, connection_id):
        table_name = SchemaTables.JIRA_ISSUES.table_name(connection_id)
        sql = f"SELECT * FROM `{table_name}` WHERE `issue_id` = {issue_id}"
        response = self.seadb_api.query_rows(self.base_id, sql)
        if response and 'results' in response and response['results']:
            return response['results'][0]
        return {}

    def save_or_update_issue_to_db(self, connection_id, issue, existed_issue):
        fields_data = issue.get('fields', {}) or {}

        content = normalize_jira_text(fields_data.get('description'))
        now_datetime = datetime.datetime.now(datetime.UTC).isoformat()

        if content and len(content) > MAX_CONTENT_LENGTH:
            content = content[:MAX_CONTENT_LENGTH]

        insert_row = {
            SchemaTables.JIRA_ISSUES.column.issue_id.name: int(issue.get('id')),
            SchemaTables.JIRA_ISSUES.column.issue_key.name: issue.get('key'),
            SchemaTables.JIRA_ISSUES.column.title.name: fields_data.get('summary'),
            SchemaTables.JIRA_ISSUES.column.content.name:content,
            SchemaTables.JIRA_ISSUES.column.status.name: (fields_data.get('status') or {}).get('name'),
            SchemaTables.JIRA_ISSUES.column.priority.name: (fields_data.get('priority') or {}).get('name'),
            SchemaTables.JIRA_ISSUES.column.assignees.name: (fields_data.get('assignee') or {}).get('displayName'),
            SchemaTables.JIRA_ISSUES.column.issue_type.name: (fields_data.get('issuetype') or {}).get('name'),
            SchemaTables.JIRA_ISSUES.column.due_date.name: fields_data.get('duedate'),
            SchemaTables.JIRA_ISSUES.column.created_time.name: time_to_utc_time(fields_data.get('created')).isoformat(),
            SchemaTables.JIRA_ISSUES.column.modified_time.name: time_to_utc_time(fields_data.get('updated')).isoformat(),
            SchemaTables.JIRA_ISSUES.column.sync_time.name: now_datetime,
            SchemaTables.JIRA_ISSUES.column.record_modified_time.name: now_datetime,
            SchemaTables.JIRA_ISSUES.column.deleted.name: False,
        }

        table_name = SchemaTables.JIRA_ISSUES.table_name(connection_id)
        if existed_issue:
            _pk = existed_issue.get('_pk')
            update_row = {
                "pk": _pk,
                "row": insert_row,
            }
            self.seadb_api.update_rows(self.base_id, table_name, [update_row])
        else:
            self.seadb_api.insert_rows(self.base_id, table_name, [insert_row])

    def save_or_update_issue_comments(self, connection_id, issue_id, comments):
        offset = 0
        page_size = 1000
        old_comment_ids = set()
        new_comment_ids = set()
        comment_id_to_pk = {}
        while True:
            old_issue_comments = self.get_issue_comments_by_page(connection_id, issue_id, page_size, offset)
            for comment in old_issue_comments:
                comment_id = comment.get('comment_id')
                old_comment_ids.add(comment_id)
                comment_id_to_pk[comment_id] = comment.get('_pk')
            if not old_issue_comments or len(old_issue_comments) < page_size:
                break
            offset += page_size
        for comment in comments:
            comment_id = int(comment.get('id'))
            new_comment_ids.add(comment_id)
            issue_id = int(issue_id)
            content = comment.get('body', {}).get('content')

            insert_row = {
                SchemaTables.JIRA_ISSUE_COMMENTS.column.comment_id.name: int(comment_id),
                SchemaTables.JIRA_ISSUE_COMMENTS.column.issue_id.name: issue_id,
                SchemaTables.JIRA_ISSUE_COMMENTS.column.author.name: (comment.get('author') or {}).get('displayName'),
                SchemaTables.JIRA_ISSUE_COMMENTS.column.content.name: normalize_jira_text(content),
                SchemaTables.JIRA_ISSUE_COMMENTS.column.created_time.name: time_to_utc_time(comment.get('created')).isoformat(),
                SchemaTables.JIRA_ISSUE_COMMENTS.column.modified_time.name: time_to_utc_time(comment.get('updated')).isoformat(),
            }

            table_name = SchemaTables.JIRA_ISSUE_COMMENTS.table_name(connection_id)
            pk = comment_id_to_pk.get(comment_id)
            if pk:
                update_row = [
                    {
                        "pk": pk,
                        "row": insert_row,
                    }
                ]
                self.seadb_api.update_rows(self.base_id, table_name, update_row)
            else:
                self.seadb_api.insert_rows(self.base_id, table_name, [insert_row])

        need_deleted_comment_ids = old_comment_ids - new_comment_ids
        for comment_id in need_deleted_comment_ids:
            self.delete_comment_from_db(connection_id, comment_id)

    def get_issues_by_page(self, connection_id, last_indexed_at, page_size, offset):
        issues_table_name = SchemaTables.JIRA_ISSUES.table_name(connection_id)
        sql = f"""SELECT * FROM `{issues_table_name}` 
                WHERE record_modified_time >= '{last_indexed_at}' 
                LIMIT {page_size} OFFSET {offset}"""
        res = self.seadb_api.query_rows(self.base_id, sql)
        issues = res.get('results', [])
        return issues

    def get_need_ai_issues_by_page(self, connection_id, page_size):
        issues_table_name = SchemaTables.JIRA_ISSUES.table_name(connection_id)
        sql = f"""
            SELECT * FROM `{issues_table_name}`
            WHERE (ai_processed_time IS NULL OR ai_processed_time < sync_time)
            AND (deleted IS NULL OR deleted = false)
            AND (outdated IS NULL OR outdated = false)
            LIMIT {page_size}
        """
        res = self.seadb_api.query_rows(self.base_id, sql)
        issues = res.get('results', [])
        return issues

    def delete_comment_from_db(self, connection_id, comment_id):
        comments_table_name = SchemaTables.JIRA_ISSUE_COMMENTS.table_name(connection_id)
        sql = f"DELETE FROM `{comments_table_name}` WHERE comment_id = {comment_id}"
        self.seadb_api.query_rows(self.base_id, sql)

    def get_jira_issues_count(self, connection_id):
        table_name = SchemaTables.JIRA_ISSUES.table_name(connection_id)
        sql = f"SELECT COUNT(*) as count FROM `{table_name}`"
        response = self.seadb_api.query_rows(self.base_id, sql)
        issues_count = 0
        if response and 'results' in response and response['results']:
            issues_count = response['results'][0].get('count', 0)
        return issues_count

    def get_issue_comments_by_page(self, connection_id, issue_id, page_size, offset):
        comments_table_name = SchemaTables.JIRA_ISSUE_COMMENTS.table_name(connection_id)
        sql_comments = f"SELECT * FROM `{comments_table_name}` WHERE issue_id = {issue_id} LIMIT {page_size} OFFSET {offset}"
        comments = self.seadb_api.query_rows(self.base_id, sql_comments).get('results', [])
        return comments
