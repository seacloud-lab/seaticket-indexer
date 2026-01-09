import logging
import json

from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
from seaqa_indexer.mq import get_mq
from seaqa_indexer.seadb_models.github_seadb_api import GitHubSeaDBAPI
from seaqa_indexer.utils.seaqa_db import SeaqaDB

logger = logging.getLogger(__name__)


class GithubIssuesSyncManager:
    def __init__(self):
        self.db_session_class = init_db_session_class()
        self.mq = get_mq(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)
        self.seaqa_db = SeaqaDB(self.db_session_class)

    def update_github_issues_by_webhook(self, connection_id, update_data, issue_id, action, event):
        project_connection = self.seaqa_db.get_project_connection_by_id(connection_id)
        project_uuid = project_connection.project_uuid
        github_seadb_api = GitHubSeaDBAPI(project_uuid)

        if event == "issues":
            logger.info(f"Received webhook for issue #{issue_id}, action: {action}, event: {event}")
            modified_time = update_data.get("updated_at")
            if action == 'deleted':
                github_seadb_api.update_issue_record(connection_id, issue_id, modified_time, deleted=True)
                self.add_issues_index_task(connection_id)
                return
            existed_issue = github_seadb_api.get_issue_by_issue_id(issue_id, connection_id)
            github_seadb_api.save_or_update_issue_to_db(connection_id, update_data, existed_issue)

        elif event == "issue_comment":
            logger.info(f"Received webhook for issue_comment #{issue_id}, action: {action}, event: {event}")
            issue_data = update_data.get('issue')
            comment_count = issue_data.get('comments')
            comment_data = update_data.get('comment')
            modified_time = comment_data.get('updated_at')

            if action == 'deleted':
                comment_id = comment_data['id']
                github_seadb_api.delete_comment_from_db(connection_id, comment_id)
                github_seadb_api.update_issue_record(connection_id, issue_id, modified_time, comment_count)
                self.add_issues_index_task(connection_id)
                return

            github_seadb_api.save_or_update_issue_comment_to_db(connection_id, issue_id, comment_data)
            github_seadb_api.update_issue_record(connection_id, issue_id, modified_time, comment_count)

        self.add_issues_index_task(connection_id)

    def add_github_issues_sync_task(self, connection_id):
        logger.info('add github issues sync task: %s', connection_id)
        msg_content = {
            'connection_id': connection_id,
        }
        self.mq.lpush('sync_task', json.dumps(msg_content))

    def add_issues_index_task(self, connection_id):
        msg_content = {
            'connection_id': connection_id,
            'type': 'connection'
        }
        self.mq.lpush('index_task', json.dumps(msg_content))
