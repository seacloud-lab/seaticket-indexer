import datetime
import json

from seaqa_indexer.utils import time_to_utc_time
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.seadb_models.models import LinearIssueTable, LinearCommentsTable

MAX_CONTENT_LENGTH = 10000


class LinearSeaDBAPI:
    def __init__(self, base_id):
        self.base_id = base_id
        self.seadb_api = SeaDBAPI()

    def get_linear_issues_count(self, connection_id):
        table_name = LinearIssueTable.gen_table_name(connection_id)
        sql = f"SELECT COUNT(*) as count FROM `{table_name}`"
        response = self.seadb_api.query_rows(self.base_id, sql)
        issues_count = 0
        if response and response.get('results'):
            issues_count = response['results'][0].get('count', 0)
        return issues_count

    def get_issue_by_issue_id(self, issue_id, connection_id):
        table_name = LinearIssueTable.gen_table_name(connection_id)
        issue_id = str(issue_id)
        sql = f"SELECT `_pk`, `issue_id`, `modified_time`, `deleted` FROM `{table_name}` WHERE `issue_id` = '{issue_id}'"
        response = self.seadb_api.query_rows(self.base_id, sql)
        if response and response.get('results'):
            return response['results'][0]
        return {}

    def get_label_column_info(self, connection_id):
        table_name = LinearIssueTable.gen_table_name(connection_id)
        metadata = self.seadb_api.get_base_metadata(self.base_id)
        table_info = {}
        for table in metadata.get('tables', []):
            if table.get('name') == table_name:
                table_info = table
                break
        table_id = table_info.get('id')
        columns = table_info.get('columns', []) or []
        label_column = {}
        for column in columns:
            if column.get('name') == LinearIssueTable.labels.name:
                label_column = column
                break

        existing_label_names = set()
        if label_column.get('data'):
            for option in label_column.get('data', {}).get('options', []):
                if option.get('name'):
                    existing_label_names.add(option['name'])

        return table_id, label_column.get('key'), existing_label_names

    def get_state_column_info(self, connection_id):
        table_name = LinearIssueTable.gen_table_name(connection_id)
        metadata = self.seadb_api.get_base_metadata(self.base_id)
        table_info = {}
        for table in metadata.get('tables', []):
            if table.get('name') == table_name:
                table_info = table
                break
        table_id = table_info.get('id')
        columns = table_info.get('columns', []) or []
        state_column = {}
        for column in columns:
            if column.get('name') == LinearIssueTable.state.name:
                state_column = column
                break

        existing_state_names = set()
        if state_column.get('data'):
            for option in state_column.get('data', {}).get('options', []):
                if option.get('name'):
                    existing_state_names.add(option['name'])

        return table_id, state_column.get('key'), existing_state_names

    def add_label_options(self, table_id, column_key, existing_label_names, labels_nodes):
        if not table_id or not column_key:
            return existing_label_names
        for label in labels_nodes:
            label_name = label.get('name')
            if not label_name or label_name in existing_label_names:
                continue
            color = label.get('color')
            option_data = {'color': f"{color}"} if color else None
            column_option = {
                'table_id': table_id,
                'column_key': column_key,
                'option_name': label_name,
                'option_data': option_data or {},
            }
            self.seadb_api.add_column_option(self.base_id, column_option)
            existing_label_names.add(label_name)
        return existing_label_names

    def add_state_options(self, table_id, column_key, existing_state_names, states_nodes):
        if not table_id or not column_key:
            return existing_state_names
        for state in states_nodes:
            state_name = state.get('name')
            if not state_name or state_name in existing_state_names:
                continue
            color = state.get('color')
            option_data = {'color': f"{color}"} if color else None
            column_option = {
                'table_id': table_id,
                'column_key': column_key,
                'option_name': state_name,
                'option_data': option_data or {},
            }
            self.seadb_api.add_column_option(self.base_id, column_option)
            existing_state_names.add(state_name)
        return existing_state_names

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
            LinearIssueTable.issue_id.name: issue_id,
            LinearIssueTable.identifier.name: issue.get('identifier'),
            LinearIssueTable.title.name: title,
            LinearIssueTable.content.name: content,
            LinearIssueTable.state.name: (issue.get('state') or {}).get('name'),
            LinearIssueTable.author.name: author,
            LinearIssueTable.assignees.name: json.dumps(assignees_list),
            LinearIssueTable.created_time.name: time_to_utc_time(issue.get('createdAt')).isoformat() if issue.get('createdAt') else None,
            LinearIssueTable.modified_time.name: time_to_utc_time(issue.get('updatedAt')).isoformat() if issue.get('updatedAt') else None,
            LinearIssueTable.sync_time.name: datetime.datetime.now(datetime.UTC).isoformat(),
            LinearIssueTable.record_modified_time.name: datetime.datetime.now(datetime.UTC).isoformat(),
            LinearIssueTable.closed_time.name: time_to_utc_time(issue.get('completedAt')).isoformat() if issue.get('completedAt') else None,
            LinearIssueTable.due_date.name: issue.get('dueDate'),
            LinearIssueTable.labels.name: label_names if label_names else None,
            LinearIssueTable.priority.name: issue.get('priority'),
            LinearIssueTable.deleted.name: False,
        }

        table_name = LinearIssueTable.gen_table_name(connection_id)
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
        table_name = LinearIssueTable.gen_table_name(connection_id)
        issue_id = str(issue_id)
        sql = f"SELECT _pk FROM `{table_name}` WHERE `issue_id` = '{issue_id}'"
        issues = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
        if not issues:
            return False
        _pk = issues[0].get('_pk')
        update_row = {
            "pk": _pk,
            "row": {
                LinearIssueTable.deleted.name: True,
                LinearIssueTable.sync_time.name: datetime.datetime.now(datetime.UTC).isoformat(),
                LinearIssueTable.record_modified_time.name: datetime.datetime.now(datetime.UTC).isoformat(),
            }
        }
        self.seadb_api.update_rows(self.base_id, table_name, [update_row])
        return True

    def mark_issue_active(self, connection_id, issue_id):
        table_name = LinearIssueTable.gen_table_name(connection_id)
        issue_id = str(issue_id)
        sql = f"SELECT _pk FROM `{table_name}` WHERE `issue_id` = '{issue_id}'"
        issues = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
        if not issues:
            return False
        _pk = issues[0].get('_pk')
        update_row = {
            "pk": _pk,
            "row": {
                LinearIssueTable.deleted.name: False,
                LinearIssueTable.sync_time.name: datetime.datetime.now(datetime.UTC).isoformat(),
                LinearIssueTable.record_modified_time.name: datetime.datetime.now(datetime.UTC).isoformat(),
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
                LinearCommentsTable.comment_id.name: comment_id,
                LinearCommentsTable.content.name: content,
                LinearCommentsTable.issue_id.name: str(issue_id),
                LinearCommentsTable.author.name: (comment.get('user') or {}).get('name'),
                LinearCommentsTable.created_time.name: time_to_utc_time(comment.get('createdAt')).isoformat() if comment.get('createdAt') else None,
                LinearCommentsTable.modified_time.name: time_to_utc_time(comment.get('updatedAt')).isoformat() if comment.get('updatedAt') else None,
            }

            comments_table_name = LinearCommentsTable.gen_table_name(connection_id)
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
        comments_table_name = LinearCommentsTable.gen_table_name(connection_id)
        comment_id_str = str(comment_id)
        sql = f"DELETE FROM `{comments_table_name}` WHERE comment_id = '{comment_id_str}'"
        self.seadb_api.query_rows(self.base_id, sql)

    def get_issues_by_page(self, connection_id, last_indexed_at, page_size, offset):
        issues_table_name = LinearIssueTable.gen_table_name(connection_id)
        if last_indexed_at:
            sql = f"""SELECT `_pk`, `issue_id`, `deleted`, `outdated`, `content`, `title`, `modified_time` FROM `{issues_table_name}` 
                    WHERE modified_time >= '{last_indexed_at}' 
                    LIMIT {page_size} OFFSET {offset}"""
        else:
            sql = f"""SELECT `_pk`, `issue_id`, `deleted`, `outdated`, `content`, `title`, `modified_time` FROM `{issues_table_name}` 
                    LIMIT {page_size} OFFSET {offset}"""
        res = self.seadb_api.query_rows(self.base_id, sql)
        return res.get('results', [])

    def get_issue_ids_by_page(self, connection_id, page_size, offset):
        issues_table_name = LinearIssueTable.gen_table_name(connection_id)
        sql = f"""SELECT issue_id, deleted FROM `{issues_table_name}` 
                LIMIT {page_size} OFFSET {offset}"""
        res = self.seadb_api.query_rows(self.base_id, sql)
        return res.get('results', [])

    def get_issue_comments_by_page(self, connection_id, issue_id, page_size, offset):
        comments_table_name = LinearCommentsTable.gen_table_name(connection_id)
        issue_id = str(issue_id)
        sql = f"SELECT `_pk`, `comment_id`, `content` FROM `{comments_table_name}` WHERE issue_id = '{issue_id}' LIMIT {page_size} OFFSET {offset}"
        comments = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
        return comments

    def get_need_ai_issues_by_page(self, connection_id, page_size):
        issues_table_name = LinearIssueTable.gen_table_name(connection_id)
        sql = f"""
            SELECT `_pk`, `title`, `content` FROM `{issues_table_name}`
            WHERE (ai_processed_time IS NULL OR ai_processed_time < sync_time) AND (deleted IS NULL OR deleted = false)
            LIMIT {page_size}
        """
        res = self.seadb_api.query_rows(self.base_id, sql)
        return res.get('results', [])
