import os
import time
import logging
import json
import threading
import signal
from redis.exceptions import ConnectionError as NoMQAvailable, ResponseError, TimeoutError

from seaqa_indexer import config
from seaqa_indexer.app.log import LogConfigurator
from seaqa_indexer.mq import get_mq
from seaqa_indexer.crawler.storage import get_storage
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.ai_processor.ai_processor import generate_summaries_and_vectors


logger = logging.getLogger(__name__)


class AIProcessTaskWorker(object):

    def __init__(self):
        self.should_stop = threading.Event()
        self.worker_num = 2

        self.LOCK_TIMEOUT = 1800
        self.REFRESH_INTERVAL = 600
        self.locked_keys = set()

        self.mq = get_mq(config.REDIS_HOST, config.REDIS_PORT, config.REDIS_PASSWORD)
        self.storage = get_storage()
        self.seadb_api = SeaDBAPI()

        self.set_signal()
        self.worker_list = []

    @property
    def tname(self):
        return threading.current_thread().name

    def _get_ai_process_lock_key(self, connection_id):
        return 'ai_process_' + str(connection_id)

    def clear_worker(self):
        for th in self.worker_list:
            th.join()
        logger.info("All AI process task threads has stopped.")

    def start(self):
        if not self.mq:
            return
        for i in range(int(self.worker_num)):
            t = threading.Thread(target=self.ai_process_task_worker, name='ai_process_task_worker_thread_' + str(i), daemon=True)
            t.start()

        threading.Thread(target=self.refresh_lock, name='refresh_thread', daemon=True).start()

    def ai_process_task_worker(self):
        logger.info('%s starting AI process task worker' % self.tname)
        try:
            while not self.should_stop.is_set():
                try:
                    res = self.mq.brpop('ai_process_task', timeout=30)
                    if res is not None:
                        key, value = res
                        try:
                            data = json.loads(value)
                        except:
                            data = None
                        if not data:
                            logger.warning('ai_process_task: invalid.', res)
                        else:
                            connection_id = data.get('connection_id')
                            project_uuid = data.get('project_uuid')
                            connection_type = data.get('connection_type')
                            self.work_task_handler(connection_id, project_uuid, connection_type)
                except (ResponseError, NoMQAvailable, TimeoutError) as e:
                    logger.error('The connection to the redis server failed: %s' % e)
        except Exception as e:
            logger.error('%s AI process project connection Error' % self.tname)
            logger.error(e, exc_info=True)
            time.sleep(0.3)

    def work_task_handler(self, connection_id, project_uuid, connection_type):
        if not self.should_stop.is_set():
            if self.mq.set(self._get_ai_process_lock_key(connection_id), time.time(),
                      ex=self.LOCK_TIMEOUT, nx=True):
                logger.info('%s start AI process project connection: %s, type: %s' % (self.tname, connection_id, connection_type))
                lock_key = self._get_ai_process_lock_key(connection_id)
                self.locked_keys.add(lock_key)
                self.process_ai_summary(connection_id, project_uuid, connection_type)
                try:
                    self.locked_keys.remove(lock_key)
                except KeyError:
                    logger.error("%s is already removed. SHOULD NOT HAPPEN!" % lock_key)
                self.mq.delete(lock_key)
                logger.info(
                    "%s Finish AI process connection: %s, delete redis lock %s" % (self.tname, connection_id, lock_key))
            else:
                self.add_to_undo_task(connection_id, project_uuid, connection_type)

    def process_ai_summary(self, connection_id, project_uuid, connection_type):
        try:
            generate_summaries_and_vectors(self.seadb_api, self.storage, connection_id, project_uuid, connection_type)
            self.send_to_index_queue(connection_id)
        except Exception as e:
            logger.exception('AI process connection: %s failed, error: %s', connection_id, e)

    def send_to_index_queue(self, connection_id):
        try:
            msg_content = {
                'connection_id': connection_id,
            }
            self.mq.lpush('index_task', json.dumps(msg_content))
        except Exception as e:
            logger.error('Failed to send connection %s to index queue: %s', connection_id, e)

    def add_to_undo_task(self, connection_id, project_uuid, connection_type):
        time.sleep(0.5)

        msg_content = {
            'connection_id': connection_id,
            'project_uuid': project_uuid,
            'connection_type': connection_type,
        }
        self.mq.lpush('ai_process_task', json.dumps(msg_content))
        logger.info('%s push back AI process task connection_id: %s, to the queue' %
                     (threading.current_thread().name, connection_id))

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
        logger.info("Exit AI process worker process")
        os._exit(0)

    def signal_term_handler(self, signal, frame):
        self.clear()

    def set_signal(self):
        signal.signal(signal.SIGTERM, self.signal_term_handler)


def start():
    ai_process_task_worker = AIProcessTaskWorker()
    logger.info("AI process task worker process initialized.")
    try:
        ai_process_task_worker.start()
    except Exception as e:
        logger.exception(e)
        ai_process_task_worker.clear()
        os._exit(0)

    while True:
        time.sleep(2)


def main():
    LogConfigurator(config.LOG_LEVEL, config.LOG_FILE)
    start()


if __name__ == "__main__":
    main()
