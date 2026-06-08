import datetime
import json

from seaqa_indexer.utils import time_to_utc_time
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.seadb_models.models import SchemaTables

MAX_CONTENT_LENGTH = 10000


class LinearSeaDBAPI:
    def __init__(self, base_id):
        self.base_id = base_id
        self.seadb_api = SeaDBAPI()

    def get_linear_issues_count(self, connection_id):
        table_name = SchemaTables.LINEAR_ISSUES.table_name(connection_id)
        sql = f"SELECT COUNT(*) as count FROM `{table_name}`"
        response = self.seadb_api.query_rows(self.base_id, sql)
        issues_count = 0
        if response and response.get('results'):
            issues_count = response['results'][0].get('count', 0)
        return issues_count

    def get_table_metadata(self, connection_id):
        table_name = SchemaTables.LINEAR_ISSUES.table_name(connection_id)
        metadata = self.seadb_api.get_base_metadata(self.base_id)
        for table in metadata.get('tables', []):
            if table.get('name') == table_name:
                return table
        return {}

    def get_issue_by_issue_id(self, issue_id, connection_id):
        table_name = SchemaTables.LINEAR_ISSUES.table_name(connection_id)
        sql = f"SELECT `_pk`, `issue_id`, `modified_time`, `deleted` FROM `{table_name}` WHERE `issue_id` = '{issue_id}'"
        response = self.seadb_api.query_rows(self.base_id, sql)
        if response and response.get('results'):
            return response['results'][0]
        return {}

    def get_label_column_info(self, connection_id):
        table_info = self.get_table_metadata(connection_id)
        table_id = table_info.get('id')
        columns = table_info.get('columns', []) or []
        label_column = {}
        for column in columns:
            if column.get('name') == SchemaTables.LINEAR_ISSUES.column.labels.name:
                label_column = column
                break

        existing_label_names = set()
        if label_column.get('data'):
            for option in label_column.get('data', {}).get('options', []):
                if option.get('name'):
                    existing_label_names.add(option['name'])

        return table_id, label_column.get('key'), existing_label_names

    def get_state_column_info(self, connection_id):
        table_info = self.get_table_metadata(connection_id)
        table_id = table_info.get('id')
        columns = table_info.get('columns', []) or []
        state_column = {}
        for column in columns:
            if column.get('name') == SchemaTables.LINEAR_ISSUES.column.state.name:
                state_column = column
                break

        existing_state_names = set()
        if state_column.get('data'):
            for option in state_column.get('data', {}).get('options', []):
                if option.get('name'):
                    existing_state_names.add(option['name'])

        return table_id, state_column.get('key'), existing_state_names

    def _sync_select_column_options(self, table_id, column_key, existing_options, remote_nodes, remote_id_key):
        if not table_id or not column_key or remote_nodes is None:
            return

        existing_remote_id_to_option = {}
        for option in existing_options:
            remote_id = option.get(remote_id_key)
            if remote_id:
                existing_remote_id_to_option[remote_id] = option

        new_remote_ids = set()
        matched_option_ids = set()
        need_added_options = []
        need_updated_options = []

        for remote_node in remote_nodes:
            remote_id = remote_node.get('id')
            remote_name = remote_node.get('name')
            remote_color = remote_node.get('color')
            if not remote_id or not remote_name:
                continue

            new_remote_ids.add(remote_id)
            old_option = existing_remote_id_to_option.get(remote_id)
            if not old_option:
                for option in existing_options:
                    if option.get('name') == remote_name and option.get('id') not in matched_option_ids:
                        old_option = option
                        break

            if not old_option:
                need_added_options.append({
                    'table_id': table_id,
                    'column_key': column_key,
                    'option_name': remote_name,
                    'option_data': {
                        'color': remote_color,
                        remote_id_key: remote_id,
                    },
                })
                continue

            option_id = old_option.get('id')
            matched_option_ids.add(option_id)
            old_remote_id = old_option.get(remote_id_key)
            old_name = old_option.get('name')
            old_color = old_option.get('color')

            update_option_data = {}
            if old_remote_id != remote_id:
                update_option_data[remote_id_key] = remote_id
            if old_color != remote_color:
                update_option_data['color'] = remote_color
            if not update_option_data:
                update_option_data = None

            new_option_name = remote_name if remote_name != old_name else None
            if new_option_name or update_option_data:
                need_updated_options.append((option_id, new_option_name, update_option_data))

        need_deleted_option_ids = []
        for option in existing_options:
            option_id = option.get('id')
            remote_id = option.get(remote_id_key)
            if remote_id and remote_id in new_remote_ids:
                continue
            if option_id in matched_option_ids:
                continue
            if option_id:
                need_deleted_option_ids.append(option_id)

        if need_deleted_option_ids:
            self.seadb_api.delete_column_option(self.base_id, table_id, column_key, option_ids=need_deleted_option_ids)

        for option_id, new_option_name, update_option_data in need_updated_options:
            self.seadb_api.update_column_option(
                self.base_id,
                table_id,
                column_key,
                option_id=option_id,
                new_option_name=new_option_name,
                update_option_data=update_option_data,
            )

        for added_option in need_added_options:
            self.seadb_api.add_column_option(self.base_id, added_option)

    def add_or_update_label_column_options(self, issue_labels, table_info):
        table_id = table_info.get('id')
        columns = table_info.get('columns', []) or []
        label_column = {}
        for column in columns:
            if column.get('name') == SchemaTables.LINEAR_ISSUES.column.labels.name:
                label_column = column
                break

        column_key = label_column.get('key')
        existing_options = (label_column.get('data') or {}).get('options', []) or []
        self._sync_select_column_options(table_id, column_key, existing_options, issue_labels, 'label_id')

    def add_or_update_state_column_options(self, states, table_info):
        table_id = table_info.get('id')
        columns = table_info.get('columns', []) or []
        state_column = {}
        for column in columns:
            if column.get('name') == SchemaTables.LINEAR_ISSUES.column.state.name:
                state_column = column
                break

        column_key = state_column.get('key')
        existing_options = (state_column.get('data') or {}).get('options', []) or []
        self._sync_select_column_options(table_id, column_key, existing_options, states, 'state_id')

    def save_or_update_issue_to_db(self, connection_id, issue, existed_issue):
        issue_id = issue.get('id')
        title = issue.get('title')
        content = issue.get('description') or ''
        if content and len(content) > MAX_CONTENT_LENGTH:
            content = content[:MAX_CONTENT_LENGTH]

        assignee = issue.get('assignee') or {}
        assignees_list = []
        if assignee.get('name'):
            assignees_list.append(assignee.get('name'))

        labels_nodes = (issue.get('labels') or {}).get('nodes', []) or []
        label_names = [label.get('name') for label in labels_nodes if label.get('name')]

        author = None
        if isinstance(issue.get('creator'), dict):
            author = issue.get('creator', {}).get('name')
        elif isinstance(issue.get('createdBy'), dict):
            author = issue.get('createdBy', {}).get('name')
        elif isinstance(issue.get('author'), dict):
            author = issue.get('author', {}).get('name')
        elif isinstance(issue.get('user'), dict):
            author = issue.get('user', {}).get('name')

        insert_row = {
            SchemaTables.LINEAR_ISSUES.column.issue_id.name: issue_id,
            SchemaTables.LINEAR_ISSUES.column.identifier.name: issue.get('identifier'),
            SchemaTables.LINEAR_ISSUES.column.title.name: title,
            SchemaTables.LINEAR_ISSUES.column.content.name: content,
            SchemaTables.LINEAR_ISSUES.column.state.name: (issue.get('state') or {}).get('name'),
            SchemaTables.LINEAR_ISSUES.column.author.name: author,
            SchemaTables.LINEAR_ISSUES.column.assignees.name: json.dumps(assignees_list),
            SchemaTables.LINEAR_ISSUES.column.created_time.name: time_to_utc_time(issue.get('createdAt')).isoformat(),
            SchemaTables.LINEAR_ISSUES.column.modified_time.name: time_to_utc_time(issue.get('updatedAt')).isoformat(),
            SchemaTables.LINEAR_ISSUES.column.sync_time.name: datetime.datetime.now(datetime.UTC).isoformat(),
            SchemaTables.LINEAR_ISSUES.column.record_modified_time.name: datetime.datetime.now(datetime.UTC).isoformat(),
            SchemaTables.LINEAR_ISSUES.column.closed_time.name: time_to_utc_time(issue.get('completedAt')).isoformat() if issue.get('completedAt') else None,
            SchemaTables.LINEAR_ISSUES.column.due_date.name: issue.get('dueDate'),
            SchemaTables.LINEAR_ISSUES.column.labels.name: label_names if label_names else None,
            SchemaTables.LINEAR_ISSUES.column.priority.name: issue.get('priority'),
            SchemaTables.LINEAR_ISSUES.column.deleted.name: False,
        }

        table_name = SchemaTables.LINEAR_ISSUES.table_name(connection_id)
        if existed_issue:
            _pk = existed_issue.get('_pk')
            update_row = {
                "pk": _pk,
                "row": insert_row,
            }
            self.seadb_api.update_rows(self.base_id, table_name, [update_row])
        else:
            self.seadb_api.insert_rows(self.base_id, table_name, [insert_row])

    def mark_issue_deleted(self, connection_id, issue_id):
        table_name = SchemaTables.LINEAR_ISSUES.table_name(connection_id)
        sql = f"SELECT _pk FROM `{table_name}` WHERE `issue_id` = '{issue_id}'"
        issues = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
        if not issues:
            return False
        _pk = issues[0].get('_pk')
        update_row = {
            "pk": _pk,
            "row": {
                SchemaTables.LINEAR_ISSUES.column.deleted.name: True,
                SchemaTables.LINEAR_ISSUES.column.sync_time.name: datetime.datetime.now(datetime.UTC).isoformat(),
                SchemaTables.LINEAR_ISSUES.column.record_modified_time.name: datetime.datetime.now(datetime.UTC).isoformat(),
            }
        }
        self.seadb_api.update_rows(self.base_id, table_name, [update_row])
        return True

    def mark_issue_active(self, connection_id, issue_id):
        table_name = SchemaTables.LINEAR_ISSUES.table_name(connection_id)
        sql = f"SELECT _pk FROM `{table_name}` WHERE `issue_id` = '{issue_id}'"
        issues = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
        if not issues:
            return False
        _pk = issues[0].get('_pk')
        update_row = {
            "pk": _pk,
            "row": {
                SchemaTables.LINEAR_ISSUES.column.deleted.name: False,
                SchemaTables.LINEAR_ISSUES.column.sync_time.name: datetime.datetime.now(datetime.UTC).isoformat(),
                SchemaTables.LINEAR_ISSUES.column.record_modified_time.name: datetime.datetime.now(datetime.UTC).isoformat(),
            }
        }
        self.seadb_api.update_rows(self.base_id, table_name, [update_row])
        return True

    def save_or_update_issue_comments(self, connection_id, issue_id, comments):
        offset = 0
        page_size = 1000
        old_comment_ids = set()
        comment_id_to_pk = {}
        while True:
            old_issue_comments = self.get_issue_comments_by_page(connection_id, issue_id, page_size, offset)
            for comment in old_issue_comments:
                comment_id = comment.get('comment_id')
                if comment_id:
                    old_comment_ids.add(comment_id)
                    comment_id_to_pk[comment_id] = comment.get('_pk')
            if not old_issue_comments or len(old_issue_comments) < page_size:
                break
            offset += page_size

        new_comment_ids = set()
        for comment in comments:
            comment_id = comment.get('id')
            if not comment_id:
                continue
            new_comment_ids.add(comment_id)
            content = comment.get('body') or ''
            if content and len(content) > MAX_CONTENT_LENGTH:
                content = content[:MAX_CONTENT_LENGTH]
            insert_row = {
                SchemaTables.LINEAR_ISSUE_COMMENTS.column.comment_id.name: comment_id,
                SchemaTables.LINEAR_ISSUE_COMMENTS.column.content.name: content,
                SchemaTables.LINEAR_ISSUE_COMMENTS.column.issue_id.name: issue_id,
                SchemaTables.LINEAR_ISSUE_COMMENTS.column.author.name: (comment.get('user') or {}).get('name'),
                SchemaTables.LINEAR_ISSUE_COMMENTS.column.created_time.name: time_to_utc_time(comment.get('createdAt')).isoformat(),
                SchemaTables.LINEAR_ISSUE_COMMENTS.column.modified_time.name: time_to_utc_time(comment.get('updatedAt')).isoformat(),
            }

            comments_table_name = SchemaTables.LINEAR_ISSUE_COMMENTS.table_name(connection_id)
            pk = comment_id_to_pk.get(comment_id)
            if pk:
                update_row = [
                    {
                        "pk": pk,
                        "row": insert_row,
                    }
                ]
                self.seadb_api.update_rows(self.base_id, comments_table_name, update_row)
            else:
                self.seadb_api.insert_rows(self.base_id, comments_table_name, [insert_row])

        need_deleted_comment_ids = old_comment_ids - new_comment_ids
        for comment_id in need_deleted_comment_ids:
            self.delete_comment_from_db(connection_id, comment_id)

    def delete_comment_from_db(self, connection_id, comment_id):
        comments_table_name = SchemaTables.LINEAR_ISSUE_COMMENTS.table_name(connection_id)
        sql = f"DELETE FROM `{comments_table_name}` WHERE comment_id = '{comment_id}'"
        self.seadb_api.query_rows(self.base_id, sql)

    def get_issues_by_page(self, connection_id, last_indexed_at, page_size, offset):
        issues_table_name = SchemaTables.LINEAR_ISSUES.table_name(connection_id)
        if last_indexed_at:
            sql = f"""SELECT `_pk`, `issue_id`, `deleted`, `outdated`, `content`, `title`, `modified_time` FROM `{issues_table_name}` 
                    WHERE record_modified_time >= '{last_indexed_at}' 
                    LIMIT {page_size} OFFSET {offset}"""
        else:
            sql = f"""SELECT `_pk`, `issue_id`, `deleted`, `outdated`, `content`, `title`, `modified_time` FROM `{issues_table_name}` 
                    LIMIT {page_size} OFFSET {offset}"""
        res = self.seadb_api.query_rows(self.base_id, sql)
        return res.get('results', [])

    def get_issue_ids_by_page(self, connection_id, page_size, offset):
        issues_table_name = SchemaTables.LINEAR_ISSUES.table_name(connection_id)
        sql = f"""SELECT issue_id, deleted FROM `{issues_table_name}` 
                LIMIT {page_size} OFFSET {offset}"""
        res = self.seadb_api.query_rows(self.base_id, sql)
        return res.get('results', [])

    def get_issue_comments_by_page(self, connection_id, issue_id, page_size, offset):
        comments_table_name = SchemaTables.LINEAR_ISSUE_COMMENTS.table_name(connection_id)
        sql = f"SELECT `_pk`, `comment_id`, `content` FROM `{comments_table_name}` WHERE issue_id = '{issue_id}' LIMIT {page_size} OFFSET {offset}"
        comments = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
        return comments

    def get_need_ai_issues_by_page(self, connection_id, page_size):
        issues_table_name = SchemaTables.LINEAR_ISSUES.table_name(connection_id)
        sql = f"""
            SELECT `_pk`, `title`, `content` FROM `{issues_table_name}`
            WHERE (ai_processed_time IS NULL OR ai_processed_time < sync_time) AND (deleted IS NULL OR deleted = false)
            LIMIT {page_size}
        """
        res = self.seadb_api.query_rows(self.base_id, sql)
        return res.get('results', [])
