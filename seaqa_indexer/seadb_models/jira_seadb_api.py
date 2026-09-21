import datetime

from seaqa_indexer.utils import time_to_utc_time
from seaqa_indexer.utils.constants import ConnectionType
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

        Order: delete stale → update existing (with conflict resolution) → add new.
        options: list of dicts with keys: id, name, color, text_color
        """
        column = self._get_column(table_metadata, column_name)
        if not column:
            return
        table_id = table_metadata.get('id')
        column_key = column.get('key')
        existing_options = (column.get('data') or {}).get('options') or []
        existing_map = {}
        old_names = set()
        for opt in existing_options:
            jira_id = opt.get('jira_id')
            opt_name = opt.get('name')
            if jira_id:
                existing_map[jira_id] = opt
                if opt_name:
                    old_names.add(opt_name)

        new_ids = set()
        need_added_options = []
        need_updated_options = []

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
                        'jira_id': opt_id,
                        'color': color,
                        'text_color': text_color,
                    },
                }
                need_added_options.append(option_payload)
                continue

            existing_name = existing.get('name')
            existing_color = existing.get('color')
            existing_text_color = existing.get('text_color')

            update_option_data = {}
            if color is not None and color != existing_color:
                update_option_data['color'] = color
            if text_color is not None and text_color != existing_text_color:
                update_option_data['text_color'] = text_color
            if update_option_data:
                update_option_data['jira_id'] = opt_id

            new_name = name if name != existing_name else None

            if new_name or update_option_data:
                need_updated_options.append(
                    (existing.get('id'), new_name, update_option_data or None, existing_name)
                )

        # 1. Delete stale options first (free up names for potential conflicts)
        need_deleted_ids = set(existing_map.keys()) - new_ids
        if need_deleted_ids:
            need_deleted_option_ids = [
                existing_map[opt_id].get('id')
                for opt_id in need_deleted_ids
                if existing_map[opt_id].get('id')
            ]
            need_deleted_option_names = {
                existing_map[opt_id].get('name')
                for opt_id in need_deleted_ids
                if existing_map[opt_id].get('name')
            }
            old_names = old_names - need_deleted_option_names
            self.seadb_api.delete_column_option(
                self.base_id, table_id, column_key, option_ids=need_deleted_option_ids,
            )

        # 2. Update existing options (with conflict resolution for name swaps, matching GitHub)
        conflict_updated_options = []
        for updated_option in need_updated_options:
            option_id, new_name, update_option_data, old_name = updated_option
            if new_name and new_name in old_names:
                conflict_updated_options.append(updated_option)
                continue
            self.seadb_api.update_column_option(
                self.base_id, table_id, column_key, option_id,
                new_option_name=new_name,
                update_option_data=update_option_data,
            )

        # 3. Resolve conflicts: use temp name to break circular name swaps
        already_update_option_names = set()
        temp_prefix = 'tmp'
        for updated_option in conflict_updated_options:
            option_id, new_name, update_option_data, old_name = updated_option
            if new_name not in already_update_option_names \
                    and old_name not in already_update_option_names:
                temp_name = f'{temp_prefix}{option_id}'
                self.seadb_api.update_column_option(
                    self.base_id, table_id, column_key,
                    option_name=new_name,
                    new_option_name=temp_name,
                    update_option_data=update_option_data,
                )
                already_update_option_names.add(new_name)

            self.seadb_api.update_column_option(
                self.base_id, table_id, column_key, option_id=option_id,
                new_option_name=new_name,
                update_option_data=update_option_data,
            )
            already_update_option_names.add(old_name)

        # 4. Add new options (names are now free after deletions)
        for added_option in need_added_options:
            self.seadb_api.add_column_option(self.base_id, added_option)

    def add_or_update_priority_column_options(self, table_metadata, priorities):
        options = []
        for priority in priorities or []:
            opt_id = priority.get('id')
            name = priority.get('name')
            status_color = priority.get('statusColor')
            if not opt_id or not name:
                continue
            options.append({
                'id': opt_id,
                'name': name,
                'color': status_color,
                'text_color': '#000000',
            })
        self._upsert_select_column_options(table_metadata, SchemaTables.JIRA_ISSUES.column.priority.name, options)

    def add_or_update_status_column_options(self, table_metadata, statuses):
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
        self._upsert_select_column_options(table_metadata, SchemaTables.JIRA_ISSUES.column.status.name, options)

    def add_or_update_issue_type_column_options(self, table_metadata, issue_types):
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
        self._upsert_select_column_options(table_metadata, SchemaTables.JIRA_ISSUES.column.issue_type.name, options)

    def get_issue_by_issue_id(self, issue_id, connection_id):
        table_name = SchemaTables.JIRA_ISSUES.table_name(connection_id)
        sql = f"""SELECT `_pk`, `issue_id`, `issue_key`, `title`, `content`, `status`, `priority`,
                `assignees`, `author`, `issue_type`, `due_date`, `created_time`, `modified_time`, `deleted`
                FROM `{table_name}` WHERE `issue_id` = {issue_id}"""
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

        assignee = (fields_data.get('assignee') or {}).get('accountId')
        assignees_list = [assignee] if assignee else []
        insert_row = {
            SchemaTables.JIRA_ISSUES.column.issue_id.name: int(issue.get('id')),
            SchemaTables.JIRA_ISSUES.column.issue_key.name: issue.get('key'),
            SchemaTables.JIRA_ISSUES.column.title.name: fields_data.get('summary'),
            SchemaTables.JIRA_ISSUES.column.content.name:content,
            SchemaTables.JIRA_ISSUES.column.status.name: (fields_data.get('status') or {}).get('name'),
            SchemaTables.JIRA_ISSUES.column.priority.name: (fields_data.get('priority') or {}).get('name'),
            SchemaTables.JIRA_ISSUES.column.assignees.name: assignees_list,
            SchemaTables.JIRA_ISSUES.column.author.name: (fields_data.get('creator') or {}).get('accountId'),
            SchemaTables.JIRA_ISSUES.column.issue_type.name: (fields_data.get('issuetype') or {}).get('name'),
            SchemaTables.JIRA_ISSUES.column.due_date.name: time_to_utc_time(fields_data.get('duedate')).isoformat() if fields_data.get('duedate') else None,
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

    def save_or_update_issue_comments(self, connection_id, issue_id, issue_key, comments):
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
            issue_id_int = int(issue_id)
            content = comment.get('body', {}).get('content')

            insert_row = {
                SchemaTables.JIRA_ISSUE_COMMENTS.column.comment_id.name: int(comment_id),
                SchemaTables.JIRA_ISSUE_COMMENTS.column.issue_id.name: issue_id_int,
                SchemaTables.JIRA_ISSUE_COMMENTS.column.issue_key.name: issue_key,
                SchemaTables.JIRA_ISSUE_COMMENTS.column.author.name: (comment.get('author') or {}).get('accountId'),
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
        sql = f"""SELECT `_pk`, `issue_id`, `deleted`, `outdated`, `content`, `title`, `modified_time` FROM `{issues_table_name}`
                WHERE record_modified_time >= '{last_indexed_at}' 
                LIMIT {page_size} OFFSET {offset}"""
        res = self.seadb_api.query_rows(self.base_id, sql)
        issues = res.get('results', [])
        return issues

    def get_need_ai_issues_by_page(self, connection_id, page_size):
        issues_table_name = SchemaTables.JIRA_ISSUES.table_name(connection_id)
        sql = f"""
            SELECT `_pk`, `title`, `content` FROM `{issues_table_name}`
            WHERE (ai_processed_time IS NULL OR ai_processed_time < record_modified_time)
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
        sql_comments = f"SELECT `_pk`, `comment_id`, `content` FROM `{comments_table_name}` WHERE issue_id = {issue_id} LIMIT {page_size} OFFSET {offset}"
        comments = self.seadb_api.query_rows(self.base_id, sql_comments).get('results', [])
        return comments
    
    def get_latest_modified_time(self, connection_id):
        table_name = SchemaTables.JIRA_ISSUES.table_name(connection_id)
        sql = f"""
            SELECT `modified_time` FROM `{table_name}`
            WHERE (deleted IS NULL OR deleted = false)
            ORDER BY `modified_time` DESC
            LIMIT 1
        """
        response = self.seadb_api.query_rows(self.base_id, sql)
        if response and 'results' in response and response['results']:
            return response['results'][0].get('modified_time')
        return None

    def upsert_user(self, connection_id, account_id, display_name):
        if not account_id or account_id == 'unknown' or not display_name:
            return

        table_name = SchemaTables.CONNECTION_USER.table_name()
        now_iso = datetime.datetime.now(datetime.UTC).isoformat()
        escaped_account_id = account_id.replace("'", "''")
        connection_type = ConnectionType.JIRA_ISSUE.value
        sql = (
            f"SELECT `_pk`, `connection_ids` FROM `{table_name}` "
            f"WHERE user_id='{escaped_account_id}' "
            f"AND connection_type='{connection_type}'"
        )
        response = self.seadb_api.query_rows(self.base_id, sql)
        existing_users = response.get('results', [])

        if existing_users:
            existing_user = existing_users[0]
            connection_ids = existing_user.get('connection_ids') or []
            row = {
                SchemaTables.CONNECTION_USER.column.name.name: display_name,
                SchemaTables.CONNECTION_USER.column.record_modified_time.name: now_iso,
            }
            if connection_id not in connection_ids:
                row[SchemaTables.CONNECTION_USER.column.connection_ids.name] = connection_ids + [connection_id]
            self.seadb_api.update_rows(self.base_id, table_name, [{
                'pk': existing_user['_pk'],
                'row': row,
            }])
            return

        self.seadb_api.insert_rows(self.base_id, table_name, [{
            SchemaTables.CONNECTION_USER.column.user_id.name: account_id,
            SchemaTables.CONNECTION_USER.column.name.name: display_name,
            SchemaTables.CONNECTION_USER.column.connection_ids.name: [connection_id],
            SchemaTables.CONNECTION_USER.column.connection_type.name: connection_type,
            SchemaTables.CONNECTION_USER.column.record_modified_time.name: now_iso,
        }])

    def get_users_needing_report(self, days_ahead):
        table_name = SchemaTables.CONNECTION_USER.table_name()
        deadline = (
            datetime.datetime.now(datetime.UTC) + datetime.timedelta(days=days_ahead)
        ).isoformat()
        results = []
        page_size = 100
        offset = 0
        connection_type = ConnectionType.JIRA_ISSUE.value

        while True:
            sql = (
                f"SELECT `_pk`, `user_id`, `name`, `connection_ids`, `record_modified_time` "
                f"FROM `{table_name}` "
                f"WHERE connection_type='{connection_type}' "
                f"AND (next_privacy_report_at IS NULL OR next_privacy_report_at <= '{deadline}') "
                f"ORDER BY `_pk` LIMIT {page_size} OFFSET {offset}"
            )
            page = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
            results.extend(page)
            if len(page) < page_size:
                return results
            offset += page_size

    def delete_users(self, pks):
        pks = [pk for pk in pks if pk is not None]
        if pks:
            self.seadb_api.delete_rows(
                self.base_id,
                SchemaTables.CONNECTION_USER.table_name(),
                list(set(pks)),
            )

    def update_user_names(self, user_names):
        if not user_names:
            return
        now_iso = datetime.datetime.now(datetime.UTC).isoformat()
        updated_users = [
            {
                'pk': pk,
                'row': {
                    SchemaTables.CONNECTION_USER.column.name.name: name,
                    SchemaTables.CONNECTION_USER.column.record_modified_time.name: now_iso,
                },
            }
            for pk, name in user_names.items()
            if pk is not None and name
        ]
        if updated_users:
            self.seadb_api.update_rows(
                self.base_id,
                SchemaTables.CONNECTION_USER.table_name(),
                updated_users,
            )

    def update_users_next_report_at(self, pks, next_report_at):
        pks = [pk for pk in pks if pk is not None]
        if not pks:
            return
        next_iso = (
            next_report_at.isoformat()
            if hasattr(next_report_at, 'isoformat')
            else str(next_report_at)
        )
        updated_users = [
            {
                'pk': pk,
                'row': {
                    SchemaTables.CONNECTION_USER.column.next_privacy_report_at.name: next_iso,
                },
            }
            for pk in set(pks)
        ]
        self.seadb_api.update_rows(
            self.base_id,
            SchemaTables.CONNECTION_USER.table_name(),
            updated_users,
        )
