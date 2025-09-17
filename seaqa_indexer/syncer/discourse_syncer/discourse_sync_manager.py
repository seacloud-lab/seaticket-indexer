import logging
import json

from sqlalchemy import text

from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.mq import get_mq
from seaqa_indexer.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
from seaqa_indexer.syncer.discourse_syncer.discourse_sync import DiscourseSync

logger = logging.getLogger(__name__)


class ForumNotFoundException(Exception):
    pass

class ForumInActiveException(Exception):
    pass


class DiscourseSyncManager:
    def __init__(self):
        self.db_session_class = init_db_session_class()
        self.mq = get_mq(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)

    def get_project_connection_by_id(self, pc_id, conn_type='discourse_forum'):
        with self.db_session_class() as session:
            sql = """SELECT pc.id, pc.config, pc.project_id, pc.status, p.uuid as project_uuid, pc.last_sync_time, is_active
            FROM project_connection pc 
            INNER JOIN projects p 
            ON p.id=pc.project_id WHERE p.deleted=false AND pc.type=:conn_type AND pc.id=:pc_id"""
            connection = session.execute(text(sql), {'pc_id': pc_id, 'conn_type': conn_type}).fetchone()
            return connection

    def add_discourse_sync_task(self, connection_id):
        msg_content = {
            'connection_id': connection_id,
        }
        self.mq.lpush('sync_task', json.dumps(msg_content))

    def add_discourse_index_task(self, connection_id):
        msg_content = {
            'connection_id': connection_id,
        }
        self.mq.lpush('index_task', json.dumps(msg_content))

    def handle_webhook(self, connection_id, request_data, event_type):
        """Handle complete webhook request processing with connection validation"""
        project_connection = self.get_project_connection_by_id(connection_id)
        project_uuid = project_connection.project_uuid
        discourse_sync = DiscourseSync(project_uuid, self.db_session_class)
        if not project_connection:
            raise ForumNotFoundException()

        connection_is_active = project_connection.is_active
        if not connection_is_active:
            logger.warning('connection: %s is inactive', connection_id)
            raise ForumInActiveException()

        topic_id = request_data.get('topic', {}).get('id')
        if event_type in ['topic_created', 'topic_recovered', 'topic_edited']:
            discourse_sync.handle_topic_update(connection_id, topic_id, request_data)
        elif event_type in ['post_created', 'post_edited', 'post_recovered']:
            discourse_sync.handle_reply_update(connection_id, topic_id, request_data)
        elif event_type == 'topic_destroyed':
            discourse_sync.handle_topic_deleted(topic_id, connection_id)
        elif event_type == 'post_destroyed':
            discourse_sync.handle_reply_delete(connection_id, topic_id, request_data)
        else:
            logger.warning(f"Unknown event type: {event_type}")
        self.add_discourse_index_task(connection_id)
