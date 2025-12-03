import os
import time
import logging
import datetime
from threading import Thread, Event

from seaqa_indexer.config import SEASEARCH_URL, SEASEARCH_TOKEN
from seaqa_indexer.utils.seasearch_api import SeaSearchAPI
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.seasearch.index_store.knowledge_base_index import KnowledgeBaseIndex
from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.utils.seaqa_db import SeaqaDB


logger = logging.getLogger(__name__)


def gen_kb_index_name(base_id):
    return f"knowledge_base_{base_id}"


def update_knowledge_base_indexes(seadb_api, seaqa_db, kb_index, projects):
    for p in projects:
        project_uuid = getattr(p, 'uuid', None) or (p[0] if isinstance(p, tuple) else None)
        since_dt = getattr(p, 'knowledge_base_indexed_at', None) or (p[1] if isinstance(p, tuple) and len(p) > 1 else None)
        if not project_uuid:
            continue
        index_name = gen_kb_index_name(project_uuid)
        try:
            kb_index.create_index_if_missing(index_name)
            indexed_count = kb_index.update(index_name, project_uuid, seadb_api, since_dt)
            now = datetime.datetime.now(datetime.UTC)
            seaqa_db.update_knowledge_base_indexed_at(project_uuid, now)
            logger.info('knowledge base %s indexed %s docs', project_uuid, indexed_count)
        except Exception as e:
            logger.exception('knowledge base %s update index failed: %s', project_uuid, e)


class KnowledgeBaseIndexUpdater(object):
    def __init__(self):
        self._interval = 1800
        self.seasearch_api = None
        self.seadb_api = None
        self.seaqa_db = None
        self.kb_index = None
        self._parse_config()

    def _parse_config(self):
        self._interval = int(os.environ.get('KB_INDEX_INTERVAL', '1800'))

        self.seasearch_api = SeaSearchAPI(SEASEARCH_URL, SEASEARCH_TOKEN)
        self.seadb_api = SeaDBAPI()
        self.seaqa_db = SeaqaDB(init_db_session_class())
        self.kb_index = KnowledgeBaseIndex(self.seasearch_api, shard_num=1)

    def start(self):
        logger.info('start knowledge base index updater, interval=%s sec', self._interval)
        KnowledgeBaseIndexUpdaterTimer(self.seadb_api, self.seaqa_db, self.kb_index, self._interval).start()


class KnowledgeBaseIndexUpdaterTimer(Thread):
    def __init__(self, seadb_api, seaqa_db, kb_index, interval):
        super(KnowledgeBaseIndexUpdaterTimer, self).__init__()
        self.seadb_api = seadb_api
        self.seaqa_db = seaqa_db
        self.kb_index = kb_index
        self.interval = interval
        self.finished = Event()

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self.interval)
            if not self.finished.is_set():
                try:
                    logger.info('start to update knowledge base indexes')
                    projects = self.seaqa_db.get_all_active_projects()
                    update_knowledge_base_indexes(self.seadb_api, self.seaqa_db, self.kb_index, projects)
                except Exception as e:
                    logger.exception('periodical update knowledge base index error: %s', e)

    def cancel(self):
        self.finished.set()
