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
        """Sync select column options using id as the unique key (matching GitHub pattern).

        options: list of dicts with keys: id, name, color, text_color
        """
        column = self._get_column(table_metadata, column_name)
        if not column:
            return
        table_id = table_metadata.get('id')
        column_key = column.get('key')
        existing_options = (column.get('data') or {}).get('options') or []
        existing_map = {}
        for opt in existing_options:
            opt_id = opt.get('id')
            if opt_id:
                existing_map[opt_id] = opt

        new_ids = set()

        for option in options:
            opt_id = option.get('id')
            name = option.get('name')
            if not opt_id or not name:
                continue
            new_ids.add(opt_id)
            color = option.get('color')
            text_color = option.get('text_color')

            existing = existing_map.get(opt_id)
            if not existing:
                option_payload = {
                    'table_id': table_id,
                    'column_key': column_key,
                    'option_name': name,
                    'option_data': {
                        'id': opt_id,
                        'color': color,
                        'text_color': text_color,
                    },
                }
                self.seadb_api.add_column_option(self.base_id, option_payload)
                continue

            existing_name = existing.get('name')
            existing_color = existing.get('color')
            existing_text_color = existing.get('text_color')

            update_option_data = {}
            if color and color != existing_color:
                update_option_data['color'] = color
            if text_color and text_color != existing_text_color:
                update_option_data['text_color'] = text_color
            if update_option_data:
                update_option_data['id'] = opt_id

            new_name = name if name != existing_name else None

            if new_name or update_option_data:
                self.seadb_api.update_column_option(
                    self.base_id, table_id, column_key,
                    option_id=existing.get('id'),
                    new_option_name=new_name,
                    update_option_data=update_option_data or None,
                )

        need_deleted_ids = set(existing_map.keys()) - new_ids
        if need_deleted_ids:
            need_deleted_names = [
                existing_map[opt_id].get('name')
                for opt_id in need_deleted_ids
                if existing_map[opt_id].get('name')
            ]
            if need_deleted_names:
                return {
                    table_id: [{
                        column_key: need_deleted_names
                    }]
                }
        return {}

    def sync_jira_priority_options(self, table_metadata, priorities):
        options = []
        for priority in priorities or []:
            opt_id = priority.get('id')
            name = priority.get('name')
            if not opt_id or not name:
                continue
            options.append({
                'id': opt_id,
                'name': name,
                'color': '#FFFFFF',
                'text_color': '#000000',
            })
        return self._upsert_select_column_options(table_metadata, SchemaTables.JIRA_ISSUES.column.priority.name, options)

    def sync_jira_status_options(self, table_metadata, statuses):
        category_colors = {
            'new': '#42526E',
            'indeterminate': '#0052CC',
            'done': '#00875A',
        }
        options = []
        for status in statuses or []:
            opt_id = status.get('id')
            name = status.get('name')
            if not opt_id or not name:
                continue
            category_key = (status.get('statusCategory') or {}).get('key')
            color = category_colors.get(category_key)
            option = {
                'id': opt_id,
                'name': name,
            }
            if color:
                option['color'] = color
                option['text_color'] = '#FFFFFF'
            options.append(option)
        return self._upsert_select_column_options(table_metadata, SchemaTables.JIRA_ISSUES.column.status.name, options)

    def sync_jira_issue_type_options(self, table_metadata, issue_types):
        options = []
        for issue_type in issue_types or []:
            opt_id = issue_type.get('id')
            name = issue_type.get('name')
            if not opt_id or not name:
                continue
            options.append({
                'id': opt_id,
                'name': name,
                'color': '#FFFFFF',
                'text_color': '#000000',
            })
        return self._upsert_select_column_options(table_metadata, SchemaTables.JIRA_ISSUES.column.issue_type.name, options)

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

        assignee = (fields_data.get('assignee') or {}).get('displayName')
        assignees_list = [assignee] if assignee else []
        insert_row = {
            SchemaTables.JIRA_ISSUES.column.issue_id.name: int(issue.get('id')),
            SchemaTables.JIRA_ISSUES.column.issue_key.name: issue.get('key'),
            SchemaTables.JIRA_ISSUES.column.title.name: fields_data.get('summary'),
            SchemaTables.JIRA_ISSUES.column.content.name:content,
            SchemaTables.JIRA_ISSUES.column.status.name: (fields_data.get('status') or {}).get('name'),
            SchemaTables.JIRA_ISSUES.column.priority.name: (fields_data.get('priority') or {}).get('name'),
            SchemaTables.JIRA_ISSUES.column.assignees.name: assignees_list,
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
