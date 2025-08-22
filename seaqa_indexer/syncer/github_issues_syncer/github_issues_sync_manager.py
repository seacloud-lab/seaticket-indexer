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

    def get_project_connection_by_id(self, connection_id):
        with self.db_session_class() as session:
            sql = """SELECT pc.id, pc.config, pc.project_id, pc.status, p.uuid as project_uuid, pc.type, pc.updated_at, pc.indexed_at, pc.is_active
             FROM project_connection pc 
            INNER JOIN projects p 
            ON p.id=pc.project_id WHERE p.deleted=false AND pc.id=:connection_id AND pc.deleted=false"""
            connection = session.execute(text(sql), {'connection_id': connection_id}).fetchone()
        return connection

    def verify_signature(self, signature, msg, github_secret):
        if not signature:
            return True
        sha_name, signature = signature.split('=')

        if sha_name != 'sha256':
            return False
        mac = hmac.new(github_secret.encode(), msg=msg, digestmod=hashlib.sha256)

        return hmac.compare_digest(mac.hexdigest(), signature)

    def update_github_issues_by_webhook(self, connection_id, issue, issue_id, action, signature, msg):
        logger.info(f"Received webhook for issue #{issue_id}, action: {action}")
        project_connection = self.get_project_connection_by_id(connection_id)
        config = json.loads(project_connection.config)
        config = decrypt_config(config)
        secret = config.get('webhook_secret')
        connection_is_active = project_connection.is_active

        if not connection_is_active:
            raise GithubIssuesInactiveException()
        if not self.verify_signature(signature, msg, secret):
            raise SignatureInvalidException()

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
