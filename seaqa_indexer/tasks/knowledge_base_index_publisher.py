import logging
import json
from threading import Thread, Event

from seaqa_indexer.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.mq import get_mq


logger = logging.getLogger(__name__)


class KnowledgeBaseIndexTaskPublisher(object):
    def __init__(self):
        self._enabled = True
        self._interval = 30 * 60

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start knowledge base index publisher: it is not enabled!')
            return

        KnowledgeBaseIndexTaskPublisherTimer(self._interval).start()
        logging.info('knowledge base index publisher is started')

    def is_enabled(self):
        return self._enabled


class KnowledgeBaseIndexTaskPublisherTimer(Thread):
    def __init__(self, interval):
        super(KnowledgeBaseIndexTaskPublisherTimer, self).__init__()
        self.db_session_class = init_db_session_class()
        self.mq = get_mq(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)
        self.seaqa_db = SeaqaDB(self.db_session_class)
        self.finished = Event()
        self._interval = interval

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self._interval)
            if self.finished.is_set():
                break
            try:
                projects = self.seaqa_db.get_all_active_projects()
                for p in projects:
                    project_uuid = getattr(p, 'uuid', None)
                    if not project_uuid:
                        continue
                    msg_content = {
                        'project_uuid': project_uuid,
                        'type': 'knowledge_base',
                    }
                    self.mq.lpush('index_task', json.dumps(msg_content))
            except Exception as e:
                logger.exception('publish kb index tasks error: %s', e)

    def cancel(self):
        self.finished.set()
