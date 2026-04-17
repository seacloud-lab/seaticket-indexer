import datetime
import json
from seaqa_indexer.utils import time_to_utc_time
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.seadb_models.models import (
    GitHubIssuesTable,
    GitHubIssueCommentsTable,
    GitHubPullRequestsTable,
    GitHubPullRequestCommentsTabel,
)

MAX_CONTENT_LENGTH = 10000


class GitHubSeaDBAPI:
    def __init__(self, base_id):
        self.base_id = base_id
        self.seadb_api = SeaDBAPI()

    def update_issue_record(self, connection_id, issue_id, modified_time, comment_count=None, deleted=False):

        row = {
            GitHubIssuesTable.modified_time.name: time_to_utc_time(modified_time).isoformat(),
            GitHubIssuesTable.sync_time.name: datetime.datetime.now(datetime.UTC).isoformat(),
            GitHubIssuesTable.record_modified_time.name: datetime.datetime.now(datetime.UTC).isoformat(),
        }

        if comment_count is not None:
            row[GitHubIssuesTable.comment_count.name] = int(comment_count)
        if deleted:
            row[GitHubIssuesTable.deleted.name] = deleted

        table_name = GitHubIssuesTable.gen_table_name(connection_id)
        sql = f"SELECT _pk FROM `{table_name}` WHERE issue_id={issue_id}"
        issues = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
        if not issues:
            return

        _pk = issues[0].get('_pk')
        update_row = {
            "pk": _pk,
            "row": row
        }
        self.seadb_api.update_rows(self.base_id, table_name, [update_row])

    def get_github_issues_count(self, connection_id):
        table_name = GitHubIssuesTable.gen_table_name(connection_id)
        sql = f"SELECT COUNT(*) as count FROM `{table_name}`"
        response = self.seadb_api.query_rows(self.base_id, sql)
        issues_count = 0
        if response and 'results' in response and response['results']:
            issues_count = response['results'][0].get('count', 0)
        return issues_count

    def save_or_update_issue_to_db(self, connection_id, issue, existed_issue):
        label_names = [label['name'] for label in issue.get('labels', []) if 'name' in label]
        assignees_list = [assignee['login'] for assignee in issue.get('assignees', []) if 'login' in assignee]
        issue_type = issue.get('type', None)
        if issue_type:
            type_name = issue_type['name']
        else:
            type_name = None

        content = issue.get('body')
        state_reason = issue.get('state_reason')

        if content and len(content) > MAX_CONTENT_LENGTH:
            content = content[:MAX_CONTENT_LENGTH]

        insert_row = {
            GitHubIssuesTable.issue_id.name: issue['id'],
            GitHubIssuesTable.issue_number.name: issue['number'],
            GitHubIssuesTable.title.name: issue.get('title'),
            GitHubIssuesTable.author.name: issue.get('user', {}).get('login', ''),
            GitHubIssuesTable.url.name: issue.get('html_url', ''),
            GitHubIssuesTable.content.name: content,
            GitHubIssuesTable.state.name: issue.get('state'),
            GitHubIssuesTable.assignees.name: json.dumps(assignees_list),
            GitHubIssuesTable.created_time.name: time_to_utc_time(issue.get('created_at')).isoformat(),
            GitHubIssuesTable.modified_time.name: time_to_utc_time(issue.get('updated_at')).isoformat(),
            GitHubIssuesTable.sync_time.name: datetime.datetime.now(datetime.UTC).isoformat(),
            GitHubIssuesTable.record_modified_time.name: datetime.datetime.now(datetime.UTC).isoformat(),
            GitHubIssuesTable.closed_time.name: time_to_utc_time(issue.get('closed_at')).isoformat() if issue.get('closed_at') else None,
            GitHubIssuesTable.comment_count.name: issue.get('comments'),
            GitHubIssuesTable.deleted.name: False,
        }

        if state_reason:
            insert_row[GitHubIssuesTable.state_reason.name] = state_reason

        insert_row[GitHubIssuesTable.labels.name] = label_names

        insert_row[GitHubIssuesTable.issue_type.name] = type_name

        table_name = GitHubIssuesTable.gen_table_name(connection_id)
        if existed_issue:
            _pk = existed_issue.get('_pk')
            update_row = {
                "pk": _pk,
                "row": insert_row,
            }
            self.seadb_api.update_rows(self.base_id, table_name, [update_row])
        else:
            self.seadb_api.insert_rows(self.base_id, table_name, [insert_row])

    def save_or_update_issue_comment_to_db(self, connection_id, issue_id, comment):
        comment_id = comment['id']
        issue_id = int(issue_id)
        content = comment.get('body', '')
        if content and len(content) > MAX_CONTENT_LENGTH:
            content = content[:MAX_CONTENT_LENGTH]

        insert_row = {
            GitHubIssueCommentsTable.comment_id.name: comment_id,
            GitHubIssueCommentsTable.issue_id.name: issue_id,
            GitHubIssueCommentsTable.author.name: comment.get('user', {}).get('login', ''),
            GitHubIssueCommentsTable.content.name: content,
            GitHubIssueCommentsTable.created_time.name: time_to_utc_time(comment.get('created_at')).isoformat(),
            GitHubIssueCommentsTable.modified_time.name: time_to_utc_time(comment.get('updated_at')).isoformat(),
        }

        table_name = GitHubIssueCommentsTable.gen_table_name(connection_id)
        sql = f"SELECT * FROM `{table_name}` WHERE comment_id={comment_id}"
        comments = self.seadb_api.query_rows(self.base_id, sql).get('results', [])
        if comments:
            _pk = comments[0].get('_pk')
            update_row = [
                {
                    "pk": _pk,
                    "row": insert_row,
                }
            ]
            self.seadb_api.update_rows(self.base_id, table_name, update_row)
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

        added_comments = []
        for comment in comments:
            comment_id = comment['id']
            new_comment_ids.add(comment_id)
            issue_id = int(issue_id)
            content = comment.get('body', '')
            if content and len(content) > MAX_CONTENT_LENGTH:
                content = content[:MAX_CONTENT_LENGTH]

            insert_row = {
                GitHubIssueCommentsTable.comment_id.name: comment_id,
                GitHubIssueCommentsTable.issue_id.name: issue_id,
                GitHubIssueCommentsTable.author.name: comment.get('user', {}).get('login', ''),
                GitHubIssueCommentsTable.content.name: content,
                GitHubIssueCommentsTable.created_time.name: time_to_utc_time(comment.get('created_at')).isoformat(),
                GitHubIssueCommentsTable.modified_time.name: time_to_utc_time(comment.get('updated_at')).isoformat(),
            }

            table_name = GitHubIssueCommentsTable.gen_table_name(connection_id)
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
                added_comments.append({
                    GitHubIssueCommentsTable.comment_id.name: comment_id,
                    GitHubIssueCommentsTable.author.name: insert_row[GitHubIssueCommentsTable.author.name],
                    GitHubIssueCommentsTable.content.name: insert_row[GitHubIssueCommentsTable.content.name],
                    GitHubIssueCommentsTable.created_time.name: insert_row[GitHubIssueCommentsTable.created_time.name],
                })

        need_deleted_comment_ids = old_comment_ids - new_comment_ids
        for comment_id in need_deleted_comment_ids:
            self.delete_comment_from_db(connection_id, comment_id)

        return added_comments

    def get_table_metadata(self, table_name):
        metadata = self.seadb_api.get_base_metadata(self.base_id)
        for table in metadata.get('tables', []):
            if table['name'] == table_name:
                table_info = table
                return table_info

    def add_or_update_issue_label_column_options(self, issue_labels, table_info):
        table_id = table_info.get('id')
        columns = table_info.get("columns", [])
        label_column = {}
        for column in columns:
            if column.get('name') == GitHubIssuesTable.labels.name:
                label_column = column
                break

        if not table_id or not label_column:
            return

        old_label_names = set()
        label_name_to_option_id = {}
        if label_column.get('data'):
            for option in label_column.get('data')['options']:
                old_label_names.add(option['name'])
                label_name_to_option_id[option['name']] = option['id']

        label_column_key = label_column.get('key')
        new_label_names = set()
        for issue_label in issue_labels:
            label_name = issue_label.get('name')
            new_label_names.add(label_name)
            if label_name not in old_label_names:
                column_key = label_column_key
                column_option = {
                    'table_id': table_id,
                    'column_key': column_key,
                    'option_name': label_name,
                    'option_data': {
                        'color': '#' + f"{issue_label.get('color')}",
                    },
                }
                self.seadb_api.add_column_option(self.base_id, column_option)
        need_deleted_labels = old_label_names - new_label_names
        if need_deleted_labels:
            need_deleted_option_ids = [label_name_to_option_id[label_name] for label_name in need_deleted_labels]
            self.seadb_api.delete_column_option(self.base_id, table_id, label_column_key, option_ids=need_deleted_option_ids)

    def add_or_update_issue_type_column_options(self, issues_types, table_info):
        color_name_to_color = {
            'gray': '#5A5F66',
            'blue': '#E0F0FF',
            'green': '#E0F9E5',
            'yellow': '#FFF9D0',
            'orange': '#FFE8D6',
            'red': '#FFE8E8',
            'pink': '#FADADD',
            'purple': '#F3E5F5',
        }
        table_id = table_info.get('id')
        columns = table_info.get("columns", [])
        issue_type_column = {}
        for column in columns:
            if column.get('name') == GitHubIssuesTable.issue_type.name:
                issue_type_column = column
                break

        old_issue_type_id_to_option = {}
        if issue_type_column.get('data'):
            issue_options = issue_type_column.get('data')['options'] or []
            for option in issue_options:
                type_id = option.get('type_id')
                old_issue_type_id_to_option[type_id] = option

        column_key = issue_type_column.get('key')
        new_issue_type_ids = set()
        for issue_type in issues_types:
            issue_type_name = issue_type.get('name')
            issue_type_id = issue_type.get('id')
            issue_type_color = issue_type.get('color').lower()

            new_issue_type_ids.add(issue_type_id)
            old_option = old_issue_type_id_to_option.get(issue_type_id)
            if not old_option:
                column_option = {
                    'table_id': table_id,
                    'column_key': column_key,
                    'option_name': issue_type_name,
                    'option_data': {
                        'color': color_name_to_color.get(issue_type_color),
                        'type_id': issue_type_id,
                    },
                }
                self.seadb_api.add_column_option(self.base_id, column_option)
            else:
                old_issue_type_name = old_option.get('name')
                old_issue_type_color = old_option.get('color')
                option_id = old_option.get('id')
                if issue_type_name != old_issue_type_name or issue_type_color != old_issue_type_color:
                    update_option_data = {
                        'color': color_name_to_color.get(issue_type_color),
                        'type_id': issue_type_id,
                    }
                    if issue_type_name == old_issue_type_name:
                        issue_type_name = None
                    if issue_type_color == old_issue_type_color:
                        update_option_data = None

                    self.seadb_api.update_column_option(self.base_id, table_id, column_key, option_id,
                                                        new_option_name=issue_type_name, update_option_data=update_option_data)

        need_deleted_issue_type_ids = set(old_issue_type_id_to_option.keys()) - new_issue_type_ids
        if need_deleted_issue_type_ids:
            need_deleted_option_ids = [old_issue_type_id_to_option[issue_type_id].get('id') for issue_type_id in need_deleted_issue_type_ids]
            self.seadb_api.delete_column_option(self.base_id, table_id, column_key, option_ids=need_deleted_option_ids)

    def get_issue_by_issue_id(self, issue_id, connection_id):
        table_name = GitHubIssuesTable.gen_table_name(connection_id)
        sql = f"SELECT * FROM `{table_name}` WHERE `issue_id` = {issue_id}"
        response = self.seadb_api.query_rows(self.base_id, sql)
        if response and 'results' in response and response['results']:
            return response['results'][0]
        return {}

    # *** github_issue index function ***
    def delete_comment_from_db(self, connection_id, comment_id):
        comments_table_name = GitHubIssueCommentsTable.gen_table_name(connection_id)
        sql = f"DELETE FROM `{comments_table_name}` WHERE comment_id = {comment_id}"
        self.seadb_api.query_rows(self.base_id, sql)

    def get_issues_by_page(self, connection_id, last_indexed_at, page_size, offset):
        issues_table_name = GitHubIssuesTable.gen_table_name(connection_id)
        sql = f"""SELECT * FROM `{issues_table_name}` 
                WHERE record_modified_time >= '{last_indexed_at}' 
                LIMIT {page_size} OFFSET {offset}"""
        res = self.seadb_api.query_rows(self.base_id, sql)
        issues = res.get('results', [])
        return issues

    def get_issue_comments_by_page(self, connection_id, issue_id, page_size, offset):
        comments_table_name = GitHubIssueCommentsTable.gen_table_name(connection_id)
        sql_comments = f"SELECT * FROM `{comments_table_name}` WHERE issue_id = {issue_id} LIMIT {page_size} OFFSET {offset}"
        comments = self.seadb_api.query_rows(self.base_id, sql_comments).get('results', [])
        return comments

    def get_need_ai_issues_by_page(self, connection_id, page_size):
        issues_table_name = GitHubIssuesTable.gen_table_name(connection_id)
        sql = f"""
            SELECT * FROM `{issues_table_name}`
            WHERE (ai_processed_time IS NULL OR ai_processed_time < sync_time) AND (deleted IS NULL OR deleted = false)
            LIMIT {page_size}
        """
        res = self.seadb_api.query_rows(self.base_id, sql)
        issues = res.get('results', [])
        return issues

    def update_issue_sync_and_record_time(self, connection_id, issue_id):
        issues_table_name = GitHubIssuesTable.gen_table_name(connection_id)
        issue = self.get_issue_by_issue_id(issue_id, connection_id)
        if not issue:
            return
        _pk = issue.get('_pk')
        updates = [{
            "pk": _pk,
            "row": {
                "sync_time": datetime.datetime.now(datetime.UTC).isoformat(),
                "record_modified_time": datetime.datetime.now(datetime.UTC).isoformat()
            }
        }]
        self.seadb_api.update_rows(self.base_id, issues_table_name, updates)

    # *** github_pull_request sync / index function ***
    def get_github_pull_requests_count(self, connection_id):
        table_name = GitHubPullRequestsTable.gen_table_name(connection_id)
        sql = f"SELECT COUNT(*) as count FROM `{table_name}`"
        response = self.seadb_api.query_rows(self.base_id, sql)
        pull_requests_count = 0
        if response and 'results' in response and response['results']:
            pull_requests_count = response['results'][0].get('count', 0)
        return pull_requests_count

    def save_or_update_pull_request_to_db(self, connection_id, pull_request, existed_pull_request):
        label_names = [label['name'] for label in pull_request.get('labels', []) if 'name' in label]
        content = pull_request.get('body')
        if content and len(content) > MAX_CONTENT_LENGTH:
            content = content[:MAX_CONTENT_LENGTH]

        merged_time = pull_request.get('merged_at')
        insert_row = {
            GitHubPullRequestsTable.pr_id.name: pull_request['id'],
            GitHubPullRequestsTable.pr_number.name: pull_request['number'],
            GitHubPullRequestsTable.title.name: pull_request.get('title'),
            GitHubPullRequestsTable.content.name: content,
            GitHubPullRequestsTable.state.name: pull_request.get('state'),
            GitHubPullRequestsTable.merged.name: bool(merged_time),
            GitHubPullRequestsTable.author.name: pull_request.get('user', {}).get('login', ''),
            GitHubPullRequestsTable.created_time.name: time_to_utc_time(pull_request.get('created_at')).isoformat(),
            GitHubPullRequestsTable.modified_time.name: time_to_utc_time(pull_request.get('updated_at')).isoformat(),
            GitHubPullRequestsTable.closed_time.name: time_to_utc_time(pull_request.get('closed_at')).isoformat() if pull_request.get('closed_at') else None,
            GitHubPullRequestsTable.merged_time.name: time_to_utc_time(merged_time).isoformat() if merged_time else None,
            GitHubPullRequestsTable.html_url.name: pull_request.get('html_url', ''),
            GitHubPullRequestsTable.deleted.name: False,
            GitHubPullRequestsTable.sync_time.name: datetime.datetime.now(datetime.UTC).isoformat(),
            GitHubPullRequestsTable.record_modified_time.name: datetime.datetime.now(datetime.UTC).isoformat(),
        }

        if label_names:
            insert_row[GitHubPullRequestsTable.labels.name] = label_names

        table_name = GitHubPullRequestsTable.gen_table_name(connection_id)
        if existed_pull_request:
            _pk = existed_pull_request.get('_pk')
            update_row = {
                "pk": _pk,
                "row": insert_row,
            }
            self.seadb_api.update_rows(self.base_id, table_name, [update_row])
        else:
            self.seadb_api.insert_rows(self.base_id, table_name, [insert_row])

    def get_pull_request_by_pr_id(self, pr_id, connection_id):
        table_name = GitHubPullRequestsTable.gen_table_name(connection_id)
        sql = f"SELECT * FROM `{table_name}` WHERE `pr_id` = {pr_id}"
        response = self.seadb_api.query_rows(self.base_id, sql)
        if response and 'results' in response and response['results']:
            return response['results'][0]
        return {}

    def delete_pull_request_comment_from_db(self, connection_id, comment_id):
        comments_table_name = GitHubPullRequestCommentsTabel.gen_table_name(connection_id)
        sql = f"DELETE FROM `{comments_table_name}` WHERE comment_id = {comment_id}"
        self.seadb_api.query_rows(self.base_id, sql)

    def save_or_update_pull_request_comments(self, connection_id, pr_id, comments):
        offset = 0
        page_size = 1000
        old_comment_ids = set()
        new_comment_ids = set()
        comment_id_to_pk = {}
        while True:
            old_pr_comments = self.get_pull_request_comments_by_page(connection_id, pr_id, page_size, offset)
            for comment in old_pr_comments:
                comment_id = comment.get('comment_id')
                old_comment_ids.add(comment_id)
                comment_id_to_pk[comment_id] = comment.get('_pk')
            if not old_pr_comments or len(old_pr_comments) < page_size:
                break
            offset += page_size

        for comment in comments:
            comment_id = comment['id']
            new_comment_ids.add(comment_id)
            content = comment.get('body', '')
            if content and len(content) > MAX_CONTENT_LENGTH:
                content = content[:MAX_CONTENT_LENGTH]

            insert_row = {
                GitHubPullRequestCommentsTabel.comment_id.name: comment_id,
                GitHubPullRequestCommentsTabel.pr_id.name: int(pr_id),
                GitHubPullRequestCommentsTabel.author.name: comment.get('user', {}).get('login', ''),
                GitHubPullRequestCommentsTabel.content.name: content,
                GitHubPullRequestCommentsTabel.created_time.name: time_to_utc_time(comment.get('created_at')).isoformat(),
                GitHubPullRequestCommentsTabel.modified_time.name: time_to_utc_time(comment.get('updated_at')).isoformat(),
            }

            table_name = GitHubPullRequestCommentsTabel.gen_table_name(connection_id)
            pk = comment_id_to_pk.get(comment_id)
            if pk:
                update_row = [{
                    "pk": pk,
                    "row": insert_row,
                }]
                self.seadb_api.update_rows(self.base_id, table_name, update_row)
            else:
                self.seadb_api.insert_rows(self.base_id, table_name, [insert_row])

        need_deleted_comment_ids = old_comment_ids - new_comment_ids
        for comment_id in need_deleted_comment_ids:
            self.delete_pull_request_comment_from_db(connection_id, comment_id)

    def get_pull_requests_by_page(self, connection_id, last_indexed_at, page_size, offset):
        prs_table_name = GitHubPullRequestsTable.gen_table_name(connection_id)
        sql = f"""SELECT * FROM `{prs_table_name}`
                WHERE record_modified_time >= '{last_indexed_at}'
                LIMIT {page_size} OFFSET {offset}"""
        res = self.seadb_api.query_rows(self.base_id, sql)
        return res.get('results', [])

    def get_pull_request_comments_by_page(self, connection_id, pr_id, page_size, offset):
        comments_table_name = GitHubPullRequestCommentsTabel.gen_table_name(connection_id)
        sql = f"SELECT * FROM `{comments_table_name}` WHERE pr_id = {pr_id} LIMIT {page_size} OFFSET {offset}"
        return self.seadb_api.query_rows(self.base_id, sql).get('results', [])

    def get_need_ai_pull_requests_by_page(self, connection_id, page_size):
        prs_table_name = GitHubPullRequestsTable.gen_table_name(connection_id)
        sql = f"""
            SELECT * FROM `{prs_table_name}`
            WHERE (ai_processed_time IS NULL OR ai_processed_time < sync_time) AND (deleted IS NULL OR deleted = false)
            LIMIT {page_size}
        """
        res = self.seadb_api.query_rows(self.base_id, sql)
        return res.get('results', [])
