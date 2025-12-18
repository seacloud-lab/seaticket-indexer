import logging
import json

from seaqa_indexer.mq import get_mq
from seaqa_indexer.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD


logger = logging.getLogger(__name__)


class KnowledgeBaseManager:
    def __init__(self):
        self.mq = get_mq(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)

    def add_knowledge_base_ai_process_and_index_task(self, knowledge_base_id, project_uuid):
        msg_content = {
            'knowledge_base_id': knowledge_base_id,
            'project_uuid': project_uuid,
            'type': 'knowledge_base',
        }
        self.mq.lpush('ai_process_task', json.dumps(msg_content))
        self.mq.lpush('index_task', json.dumps(msg_content)) # make kb search visible immediately

