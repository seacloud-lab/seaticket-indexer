import logging
import json

from seaqa_indexer.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
from seaqa_indexer.mq import get_mq

logger = logging.getLogger(__name__)


class SeafileSyncManager:
    def __init__(self):
        self.mq = get_mq(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)

    def add_sync_task(self, connection_id):
        logger.info('add seafile sync task: %s', connection_id)
        msg_content = {
            'connection_id': connection_id,
        }
        self.mq.lpush('sync_task', json.dumps(msg_content))

