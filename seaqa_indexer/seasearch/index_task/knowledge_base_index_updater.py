import os
import logging
import datetime
import json
from threading import Thread, Event

from seaqa_indexer.config import SEASEARCH_URL, SEASEARCH_TOKEN, REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
from seaqa_indexer.utils.seasearch_api import SeaSearchAPI
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.seasearch.index_store.knowledge_base_index import KnowledgeBaseIndex
from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.seasearch.utils import gen_kb_index_name
from seaqa_indexer.mq import get_mq


logger = logging.getLogger(__name__)


class KnowledgeBaseIndexTaskPublisher(object):
    def __init__(self):
        self._interval = int(os.environ.get('KB_INDEX_INTERVAL', '1800'))
        self.db_session_class = init_db_session_class()
        self.mq = get_mq(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)
        self.seaqa_db = SeaqaDB(self.db_session_class)

    def start(self):
        KnowledgeBaseIndexTaskPublisherTimer(self.mq, self.seaqa_db, self._interval).start()


class KnowledgeBaseIndexTaskPublisherTimer(Thread):
    def __init__(self, mq, seaqa_db, interval):
        super(KnowledgeBaseIndexTaskPublisherTimer, self).__init__()
        self.mq = mq
        self.seaqa_db = seaqa_db
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
