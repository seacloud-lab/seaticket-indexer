import logging
import json

from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.mq import get_mq
from seaqa_indexer.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
from seaqa_indexer.seadb_models.discourse_seadb_api import DiscourseSeaDBAPI
from seaqa_indexer.seadb_models.github_seadb_api import GitHubSeaDBAPI
from seaqa_indexer.utils.seaqa_db import SeaqaDB

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

    def add_sync_task(self, connection_id):
        logger.info('add sync task: %s', connection_id)
        msg_content = {
            'connection_id': connection_id,
        }
        self.mq.lpush('sync_task', json.dumps(msg_content))

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
        if event_type in ['topic_created', 'topic_recovered', 'topic_edited']:
            topic_data = request_data.get('topic', {})
            existing_topic = discourse_seadb_api.get_topic_by_topic_id(connection_id, topic_id)
            if topic_pk := existing_topic.get('_pk'):
                discourse_seadb_api.update_topic(connection_id, topic_pk, topic_data)
            else:
                discourse_seadb_api.insert_topic(connection_id, topic_id, topic_data)
                logger.debug(f"Created new topic {topic_id}")
        elif event_type in ['post_created', 'post_edited', 'post_recovered']:
            reply_data = request_data.get('post', {})
            post_number = reply_data.get('post_number')
            existing_reply = discourse_seadb_api.get_reply_by_id(connection_id, topic_id, post_number)
            if existing_reply:
                reply_pk = existing_reply.get('_pk')
                discourse_seadb_api.update_reply(connection_id, reply_pk, topic_id, reply_data)
            else:
                discourse_seadb_api.insert_reply(connection_id, topic_id, reply_data)
            discourse_seadb_api.update_topic_sync_and_record_time(connection_id, topic_id)
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
        self.add_index_task(connection_id)

    def update_github_issues_by_webhook(self, connection_id, update_data, issue_id, action, event):
        project_connection = self.seaqa_db.get_project_connection_by_id(connection_id)
        project_uuid = project_connection.project_uuid
        github_seadb_api = GitHubSeaDBAPI(project_uuid)

        if event == "issues":
            logger.info(f"Received webhook for issue #{issue_id}, action: {action}, event: {event}")
            modified_time = update_data.get("updated_at")
            if action == 'deleted':
                github_seadb_api.update_issue_record(connection_id, issue_id, modified_time, deleted=True)
                self.add_index_task(connection_id)
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
            else:
                github_seadb_api.save_or_update_issue_comment_to_db(connection_id, issue_id, comment_data)
            github_seadb_api.update_issue_record(connection_id, issue_id, modified_time, comment_count)

        self.add_index_task(connection_id)
