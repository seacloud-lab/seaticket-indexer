import os
import time
import logging
import json
import threading
import signal
from redis.exceptions import ConnectionError as NoMQAvailable, ResponseError, TimeoutError

from seaqa_indexer import config
from seaqa_indexer.app.log import LogConfigurator
from seaqa_indexer.seasearch.index_utils import update_connection_content_vector_index
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
        self.worker_num = 2

        self.LOCK_TIMEOUT = 1800  # 30 minutes
        self.REFRESH_INTERVAL = 600
        self.locked_keys = set()

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

    def _get_content_vector_index_lock_key(self, connection_id):
        return 'content_vector_index_' + str(connection_id)

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

        threading.Thread(target=self.refresh_lock, name='refresh_thread', daemon=True).start()

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
        if not self.should_stop.is_set():
            if self.mq.set(self._get_content_vector_index_lock_key(connection_id), time.time(),
                      ex=self.LOCK_TIMEOUT, nx=True):
                logger.info('%s start content vector index project connection: %s' % (self.tname, connection_id))
                lock_key = self._get_content_vector_index_lock_key(connection_id)
                self.locked_keys.add(lock_key)
                self.update_content_vector_index(connection_id)
                try:
                    self.locked_keys.remove(lock_key)
                except KeyError:
                    logger.error("%s is already removed. SHOULD NOT HAPPEN!" % lock_key)
                self.mq.delete(lock_key)
                logger.info(
                    "%s Finish content vector index connection: %s, delete redis lock %s" % (self.tname, connection_id, lock_key))
            else:
                logger.warning('another content vector index task is running, drop this task, connection_id: %s', connection_id)

    def update_content_vector_index(self, connection_id):
        try:
            update_connection_content_vector_index(connection_id, self.seaqa_db, self.seadb_api, self.storage, self.seasearch_api)
        except Exception as e:
            logger.exception('Content vector index connection: %s failed, error: %s', connection_id, e)

    def refresh_lock(self):
        logger.info('%s Starting refresh locks', self.tname)
        while not self.should_stop.is_set():
            try:
                copy = self.locked_keys.copy()

                for lock in copy:
                    ttl = self.mq.ttl(lock)
                    new_ttl = ttl + self.REFRESH_INTERVAL
                    self.mq.expire(lock, new_ttl)
                    logger.debug('%s Refresh lock [%s] timeout from %s to %s' %
                                 (self.tname, lock, ttl, new_ttl))
                time.sleep(self.REFRESH_INTERVAL)
            except Exception as e:
                logger.exception(e)
                time.sleep(1)

    def clear(self):
        if not self.mq:
            return
        self.should_stop.set()
        time.sleep(1)
        for key in self.locked_keys:
            self.mq.delete(key)
            logger.info("redis lock key %s has been deleted", key)
        logger.info("Exit content vector index worker process")
        os._exit(0)

    def signal_term_handler(self, signal, frame):
        self.clear()

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
