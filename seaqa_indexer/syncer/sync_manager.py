import logging
import json

from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.mq import get_mq
from seaqa_indexer.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
from seaqa_indexer.seadb_models.discourse_seadb_api import DiscourseSeaDBAPI
from seaqa_indexer.seadb_models.github_seadb_api import GitHubSeaDBAPI
from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.syncer.utils import SYNC_TASK_RATE_LIMIT_SECONDS, PENDING_SYNC_TASK_TTL

logger = logging.getLogger(__name__)

class ForumNotFoundException(Exception):
    pass

class ForumInActiveException(Exception):
    pass


class SyncManager:
    def __init__(self):
        self.db_session_class = init_db_session_class()
        self.mq = get_mq(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)
        self.seaqa_db = SeaqaDB(self.db_session_class)

    def _get_rate_limit_key(self, connection_id):
        return f'sync_task_rate_limit:{connection_id}'

    def _get_pending_key(self, connection_id):
        return f'sync_task_pending:{connection_id}'

    def add_sync_task(self, connection_id):
        rate_limit_key = self._get_rate_limit_key(connection_id)
        pending_key = self._get_pending_key(connection_id)
        
        # If the sync task is rate limited, mark it as pending.
        if not self.mq.set(rate_limit_key, 1, ex=SYNC_TASK_RATE_LIMIT_SECONDS, nx=True):
            logger.info('sync task rate limited, marking as pending, connection_id: %s', connection_id)
            # Mark it as pending, the expiration time is longer than the rate limit time, ensure enough time to trigger.
            self.mq.set(pending_key, 1, ex=PENDING_SYNC_TASK_TTL)
            return

        logger.info('add sync task: %s', connection_id)
        msg_content = {
            'connection_id': connection_id,
        }
        self.mq.lpush('sync_task', json.dumps(msg_content))
        
        self.mq.delete(pending_key)

    def check_and_trigger_pending_sync(self, connection_id):
        """Check and trigger the pending sync task.
        After the sync task is completed, call this method to check 
        if there is a pending sync request for this connection.
        If there is and the rate limit is lifted, trigger a new sync task.
        """
        pending_key = self._get_pending_key(connection_id)
        rate_limit_key = self._get_rate_limit_key(connection_id)
        
        # Check if there is a pending sync task.
        if self.mq.exists(pending_key):
            # Check if the rate limit is lifted.
            if not self.mq.exists(rate_limit_key):
                logger.info('trigger pending sync task for connection: %s', connection_id)
                self.add_sync_task(connection_id)
            else:
                logger.debug('pending sync task exists but still in rate limit period, connection: %s', connection_id)

    def add_index_task(self, connection_id):
        msg_content = {
            'connection_id': connection_id,
            'type': 'connection',
        }
        self.mq.lpush('index_task', json.dumps(msg_content))

    def handle_discourse_webhook(self, connection_id, request_data, event_type):
        """Handle complete webhook request processing with connection validation"""
        project_connection = self.seaqa_db.get_project_connection_by_id(connection_id)
        if not project_connection:
            raise ForumNotFoundException()

        project_uuid = project_connection.project_uuid
        discourse_seadb_api = DiscourseSeaDBAPI(project_uuid)

        connection_is_active = project_connection.is_active
        if not connection_is_active:
            logger.warning('connection: %s is inactive', connection_id)
            raise ForumInActiveException()

        topic_id = request_data.get('topic', {}).get('id')

        if event_type in ['topic_created', 'topic_recovered', 'topic_edited',
                'post_created', 'post_edited', 'post_recovered']:
            self.add_sync_task(connection_id)
        elif event_type == 'topic_destroyed':
            discourse_seadb_api.delete_topic(connection_id, topic_id)
        elif event_type == 'post_destroyed':
            reply_data = request_data.get('post', {})
            post_number = reply_data.get('post_number')
            if post_number:
                discourse_seadb_api.delete_reply_from_db(connection_id, topic_id, post_number)
                discourse_seadb_api.update_topic_sync_and_record_time(connection_id, topic_id)
        else:
            logger.warning(f"Unknown event type: {event_type}")

    def update_github_issues_by_webhook(self, connection_id, update_data, issue_id, action, event):
        project_connection = self.seaqa_db.get_project_connection_by_id(connection_id)
        project_uuid = project_connection.project_uuid
        github_seadb_api = GitHubSeaDBAPI(project_uuid)

        if event == "issues":
            logger.info(f"Received webhook for issue #{issue_id}, action: {action}, event: {event}")
            modified_time = update_data.get("updated_at")
            if action == 'deleted':
                github_seadb_api.update_issue_record(connection_id, issue_id, modified_time, deleted=True)
                return

        elif event == "issue_comment":
            logger.info(f"Received webhook for issue_comment #{issue_id}, action: {action}, event: {event}")
            comment_data = update_data.get('comment')

            if action == 'deleted':
                comment_id = comment_data['id']
                github_seadb_api.delete_comment_from_db(connection_id, comment_id)
                github_seadb_api.update_issue_sync_and_record_time(connection_id, issue_id)
                return

        self.add_sync_task(connection_id)
