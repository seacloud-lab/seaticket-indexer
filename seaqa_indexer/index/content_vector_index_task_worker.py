import os
import time
import logging
import json
import threading
import signal
from redis.exceptions import ConnectionError as NoMQAvailable, ResponseError, TimeoutError

from seaqa_indexer import config
from seaqa_indexer.app.log import LogConfigurator
from seaqa_indexer.index.index_utils import update_connection_content_vector_index
from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.mq import get_mq
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.crawler.storage import get_storage
from seaqa_indexer.utils.seasearch_api import SeaSearchAPI
from seaqa_indexer.config import SEASEARCH_URL, SEASEARCH_TOKEN


logger = logging.getLogger(__name__)


class ContentVectorIndexTaskWorker(object):
    """Worker for processing content vector indexing tasks"""

    def __init__(self):
        self.db_session_class = init_db_session_class()
        self.should_stop = threading.Event()
        self.worker_num = 3

        self.seadb_api = SeaDBAPI()
        self.seaqa_db = SeaqaDB(self.db_session_class)
        self.storage = get_storage()
        self.seasearch_api = SeaSearchAPI(SEASEARCH_URL, SEASEARCH_TOKEN)

        self.mq = get_mq(config.REDIS_HOST, config.REDIS_PORT, config.REDIS_PASSWORD)

        self.set_signal()
        self.worker_list = []

    @property
    def tname(self):
        return threading.current_thread().name

    def clear_worker(self):
        for th in self.worker_list:
            th.join()
        logger.info("All content vector index task threads has stopped.")

    def start(self):
        if not self.mq:
            return
        for i in range(int(self.worker_num)):
            t = threading.Thread(
                target=self.content_vector_index_task_worker,
                name='content_vector_index_task_worker_thread_' + str(i),
                daemon=True
            )
            t.start()

    def content_vector_index_task_worker(self):
        logger.info('%s starting content vector index task worker' % self.tname)
        try:
            while not self.should_stop.is_set():
                try:
                    res = self.mq.brpop('content_vector_index_task', timeout=30)
                    if res is not None:
                        key, value = res
                        try:
                            data = json.loads(value)
                        except:
                            data = None
                        if not data:
                            logger.warning('content_vector_index_task: invalid.', res)
                        else:
                            connection_id = data.get('connection_id')
                            self.work_task_handler(connection_id)
                except (ResponseError, NoMQAvailable, TimeoutError) as e:
                    logger.error('The connection to the redis server failed: %s' % e)
        except Exception as e:
            logger.error('%s content vector index project connection Error' % self.tname)
            logger.error(e, exc_info=True)
            time.sleep(0.3)

    def work_task_handler(self, connection_id):
        try:
            update_connection_content_vector_index(connection_id, self.seaqa_db, self.seadb_api, self.storage, self.seasearch_api)
        except Exception as e:
            logger.exception('Content vector index connection: %s failed, error: %s', connection_id, e)

    def signal_term_handler(self, signal, frame):
        logger.info("Exit content vector index task process")
        os._exit(0)

    def set_signal(self):
        signal.signal(signal.SIGTERM, self.signal_term_handler)


def start():
    content_vector_index_task_worker = ContentVectorIndexTaskWorker()
    logger.info("Content vector index task worker process initialized.")
    try:
        content_vector_index_task_worker.start()
    except Exception as e:
        logger.exception(e)
        content_vector_index_task_worker.clear()
        os._exit(0)

    while True:
        time.sleep(2)


def main():
    LogConfigurator(config.LOG_LEVEL, config.LOG_FILE)
    start()


if __name__ == "__main__":
    main()
