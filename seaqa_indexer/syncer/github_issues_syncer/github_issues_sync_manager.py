import logging
import json
import hmac
import hashlib

from sqlalchemy.sql import text

from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
from seaqa_indexer.mq import get_mq
from seaqa_indexer.utils import decrypt_config
from seaqa_indexer.syncer.github_issues_syncer.github_issues_sync import GithubIssuesSync

logger = logging.getLogger(__name__)


class SignatureInvalidException(Exception):
    pass


class GithubIssuesNotFoundException(Exception):
    pass


class GithubIssuesInactiveException(Exception):
    pass


class GithubIssuesSyncManager:
    def __init__(self):
        self.db_session_class = init_db_session_class()
        self.mq = get_mq(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)
        self.github_issues_sync = GithubIssuesSync(self.db_session_class)

    def update_github_issues_by_webhook(self, connection_id, issue, issue_id, action):
        logger.info(f"Received webhook for issue #{issue_id}, action: {action}")

        self.github_issues_sync.save_or_update_issues_to_db(connection_id, issue)
        if action == 'deleted':
            self.github_issues_sync.tag_issue_deleted(connection_id, issue_id)
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
        }
        self.mq.lpush('index_task', json.dumps(msg_content))
