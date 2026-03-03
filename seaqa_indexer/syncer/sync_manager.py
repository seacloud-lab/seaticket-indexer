import json
import time
import logging
from threading import Thread
from copy import deepcopy

from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.mq import get_mq
from seaqa_indexer.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
from seaqa_indexer.seadb_models.discourse_seadb_api import DiscourseSeaDBAPI
from seaqa_indexer.seadb_models.github_seadb_api import GitHubSeaDBAPI
from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.utils.constants import ConnectionType

logger = logging.getLogger(__name__)

class ForumNotFoundException(Exception):
    pass

class ForumInActiveException(Exception):
    pass


NEED_RECOVER_SYNC_TYPES = [
    ConnectionType.GITHUB_ISSUE.value,
    ConnectionType.DISCOURSE_FORUM.value,
    ConnectionType.SEAFILE.value,
    ConnectionType.EMAIL.value,
    ConnectionType.SITE.value,
]


class SyncManager(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.pending_tasks = dict()
        self.sync_interval = 5 * 60
        self.db_session_class = init_db_session_class()
        self.mq = get_mq(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)
        self.seaqa_db = SeaqaDB(self.db_session_class)

    def add_sync_task(self, connection_id):
        logger.info('add sync task: %s', connection_id)
        msg_content = {
            'connection_id': connection_id,
        }
        self.mq.lpush('sync_task', json.dumps(msg_content))

    def recover_interrupted_tasks(self):
        logger.info("Start to recover interrupted sync tasks...")
        try:
            connections = self.seaqa_db.get_connections_to_recover(NEED_RECOVER_SYNC_TYPES)
            for connection in connections:
                connection_id = connection.id
                connection_type = connection.type

                logger.info(f"Add recovering sync task for connection {connection_id}")
                if connection_type == ConnectionType.SITE.value:
                    msg_content = {
                        'site_id': connection_id,
                    }
                    self.mq.lpush('web_crawl_task', json.dumps(msg_content))
                    continue

                self.add_sync_task(connection_id)

        except Exception as e:
            logger.exception(f"Error during task recovery: {e}")

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
            self.check_and_sync(connection_id)
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

    def update_github_issues_by_webhook(self, installation_id, update_data, issue_id, action, event, repository_html_url):
        if event not in ('issues', 'issue_comment', 'installation'):
            return

        if event == 'installation' and action != 'deleted':
            return
        project_connections = self.seaqa_db.get_project_connections_by_installation_id(installation_id)
        for connection in project_connections:
            project_uuid = connection.project_uuid
            connection_id = connection.id
            if not connection.is_active:
                continue
            connection_config = json.loads(connection.config)
            if connection_config.get('repository').strip('/') != repository_html_url:
                continue
            github_seadb_api = GitHubSeaDBAPI(project_uuid)

            if event == "issues":
                logger.info(f"Received webhook for issue #{issue_id}, action: {action}, event: {event}")
                modified_time = update_data.get("updated_at")
                if action == 'deleted':
                    github_seadb_api.update_issue_record(connection_id, issue_id, modified_time, deleted=True)
                    continue

            elif event == "issue_comment":
                logger.info(f"Received webhook for issue_comment #{issue_id}, action: {action}, event: {event}")
                comment_data = update_data.get('comment')

                if action == 'deleted':
                    comment_id = comment_data['id']
                    github_seadb_api.delete_comment_from_db(connection_id, comment_id)
                    github_seadb_api.update_issue_sync_and_record_time(connection_id, issue_id)
                    continue
            elif event == 'installation' and action == 'deleted':
                pass

            self.check_and_sync(connection_id)

    def check_and_sync(self, connection_id):
        now_time = time.time()
        last_sync_time = self.pending_tasks.get(connection_id, 0)
        if (now_time - last_sync_time) >= self.sync_interval:
            self.pending_tasks[connection_id] = now_time
            self.add_sync_task(connection_id)

    def run(self):
        logger.info('sync manager thread started')
        self.recover_interrupted_tasks()
        while True:
            copied_pending_tasks = deepcopy(self.pending_tasks)
            now_time = time.time()
            logger.info('start check pending sync tasks, task count: %s', len(copied_pending_tasks))
            try:
                for connection_id, last_sync_time in copied_pending_tasks.items():
                    if (now_time - last_sync_time) >= self.sync_interval:
                        self.add_sync_task(connection_id)
                        self.pending_tasks.pop(connection_id)
            except Exception as e:
                logger.error('Error handing sync connection: %s' % e)
            time.sleep(30)
