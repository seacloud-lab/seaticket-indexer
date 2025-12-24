import os
import time
import logging
import json
import threading
import signal
from redis.exceptions import ConnectionError as NoMQAvailable, ResponseError, TimeoutError

from seaqa_indexer import config
from seaqa_indexer.config import SEASEARCH_URL, SEASEARCH_TOKEN
from seaqa_indexer.app.log import LogConfigurator
from seaqa_indexer.seasearch.summary_index_utils import update_connection_summary_index, update_knowledge_base_summary_index
from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.mq import get_mq
from seaqa_indexer.utils.seasearch_api import SeaSearchAPI
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.seasearch.summary_index_ticket import update_tickets_summary_index


logger = logging.getLogger(__name__)


class SummaryIndexTaskWorker(object):
    """Worker for processing AI summary vector indexing tasks"""

    def __init__(self):
        self.db_session_class = init_db_session_class()
        self.should_stop = threading.Event()
        self.worker_num = 2

        self.LOCK_TIMEOUT = 1800  # 30 minutes
        self.REFRESH_INTERVAL = 600
        self.locked_keys = set()

        self.seadb_api = SeaDBAPI()
        self.seaqa_db = SeaqaDB(self.db_session_class)
        self.seasearch_api = SeaSearchAPI(SEASEARCH_URL, SEASEARCH_TOKEN)

        self.mq = get_mq(config.REDIS_HOST, config.REDIS_PORT, config.REDIS_PASSWORD)

        self.set_signal()
        self.worker_list = []

    @property
    def tname(self):
        return threading.current_thread().name

    def _get_conn_summary_index_lock_key(self, connection_id):
        return 'summary_index_' + str(connection_id)

    def _get_kb_summary_index_lock_key(self, project_uuid):
        return 'summary_index_kb_' + str(project_uuid)

    def _get_ticket_summary_index_lock_key(self, project_uuid):
        return 'summary_index_ticket_' + str(project_uuid)

    def clear_worker(self):
        for th in self.worker_list:
            th.join()
        logger.info("All summary index task threads has stopped.")

    def start(self):
        if not self.mq:
            return
        for i in range(int(self.worker_num)):
            t = threading.Thread(
                target=self.summary_index_task_worker,
                name='summary_index_task_worker_thread_' + str(i),
                daemon=True
            )
            t.start()

        threading.Thread(target=self.refresh_lock, name='refresh_thread', daemon=True).start()

    def summary_index_task_worker(self):
        logger.info('%s starting summary index task worker' % self.tname)
        try:
            while not self.should_stop.is_set():
                try:
                    res = self.mq.brpop('summary_index_task', timeout=30)
                    if res is not None:
                        key, value = res
                        try:
                            data = json.loads(value)
                        except:
                            data = None
                        if not data:
                            logger.warning('summary_index_task: invalid.', res)
                        else:
                            task_type = data.get('type', 'connection')
                            if task_type == 'knowledge_base':
                                project_uuid = data.get('project_uuid')
                                self.kb_task_handler(project_uuid)
                            elif data.get('type') == 'ticket':
                                project_uuid = data.get('project_uuid')
                                self.tickets_task_handler(project_uuid)
                            elif data.get('type') == 'connection':
                                connection_id = data.get('connection_id')
                                self.work_task_handler(connection_id)
                            else:
                                logger.warning('summary_index_task: invalid.', res)
                except (ResponseError, NoMQAvailable, TimeoutError) as e:
                    logger.error('The connection to the redis server failed: %s' % e)
        except Exception as e:
            logger.error('%s summary index project connection Error' % self.tname)
            logger.error(e, exc_info=True)
            time.sleep(0.3)

    def work_task_handler(self, connection_id):
        if not self.should_stop.is_set():
            lock_key = self._get_conn_summary_index_lock_key(connection_id)
            if self.mq.set(lock_key, time.time(), ex=self.LOCK_TIMEOUT, nx=True):
                logger.info('%s start summary index project connection: %s' % (self.tname, connection_id))
                self.locked_keys.add(lock_key)
                self.update_ai_index(connection_id)
                try:
                    self.locked_keys.remove(lock_key)
                except KeyError:
                    logger.error("%s is already removed. SHOULD NOT HAPPEN!" % lock_key)
                self.mq.delete(lock_key)
                logger.info(
                    "%s Finish summary index connection: %s, delete redis lock %s" % (self.tname, connection_id, lock_key))
            else:
                self.add_to_undo_task(connection_id)

    def kb_task_handler(self, project_uuid):
        if not self.should_stop.is_set():
            lock_key = self._get_kb_summary_index_lock_key(project_uuid)
            if self.mq.set(lock_key, time.time(), ex=self.LOCK_TIMEOUT, nx=True):
                logger.info('%s start summary index knowledge base of project: %s' % (self.tname, project_uuid))
                self.locked_keys.add(lock_key)
                self.update_kb_index(project_uuid)
                try:
                    self.locked_keys.remove(lock_key)
                except KeyError:
                    logger.error("%s is already removed. SHOULD NOT HAPPEN!" % lock_key)
                self.mq.delete(lock_key)
                logger.info("%s Finish summary index knowledge base of project: %s, delete redis lock %s" %
                            (self.tname, project_uuid, lock_key))
            else:
                self.add_kb_to_undo_task(project_uuid)

    def tickets_task_handler(self, project_uuid):
        if not self.should_stop.is_set():
            lock_key = self._get_ticket_summary_index_lock_key(project_uuid)
            if self.mq.set(lock_key, time.time(), ex=self.LOCK_TIMEOUT, nx=True):
                logger.info('%s start summary index tickets: %s' % (self.tname, project_uuid))
                self.locked_keys.add(lock_key)
                self.update_tickets_ai_index(project_uuid)
                try:
                    self.locked_keys.remove(lock_key)
                except KeyError:
                    logger.error("%s is already removed. SHOULD NOT HAPPEN!" % lock_key)
                self.mq.delete(lock_key)
                logger.info(
                    "%s Finish summary index tickets: %s, delete redis lock %s" % (self.tname, project_uuid, lock_key))
            else:
                self.add_tickets_to_undo_task(project_uuid)

    def update_tickets_ai_index(self, project_uuid):
        try:
            update_tickets_summary_index(project_uuid, self.seaqa_db, self.seadb_api, self.seasearch_api)
        except Exception as e:
            logger.exception('Summary index tickets: %s failed, error: %s', project_uuid, e)

    def add_tickets_to_undo_task(self, project_uuid):
        time.sleep(0.5)
        msg_content = {
            'project_uuid': project_uuid,
            'type': 'ticket',
        }
        self.mq.lpush('summary_index_task', json.dumps(msg_content))
        logger.info('%s push back summary index task project_uuid: %s, to the queue' % (self.tname, project_uuid))

    def update_ai_index(self, connection_id):
        try:
            update_connection_summary_index(connection_id, self.seaqa_db, self.seadb_api, self.seasearch_api)
        except Exception as e:
            logger.exception('Summary index connection: %s failed, error: %s', connection_id, e)

    def update_kb_index(self, project_uuid):
        try:
            update_knowledge_base_summary_index( project_uuid, self.seaqa_db, self.seadb_api, self.seasearch_api)
        except Exception as e:
            logger.exception('Summary index knowledge base of project: %s failed, error: %s', project_uuid, e)

    def add_to_undo_task(self, connection_id):
        """Push task back to the end of the queue."""
        time.sleep(0.5)

        msg_content = {
            'connection_id': connection_id,
        }
        self.mq.lpush('summary_index_task', json.dumps(msg_content))
        logger.info('%s push back summary index task connection_id: %s, to the queue' %
                     (threading.current_thread().name, connection_id))

    def add_kb_to_undo_task(self, project_uuid):
        time.sleep(0.5)

        msg_content = {
            'project_uuid': project_uuid,
            'type': 'knowledge_base',
        }
        self.mq.lpush('summary_index_task', json.dumps(msg_content))
        logger.info('%s push back summary index kb task project_uuid: %s, to the queue' %
                     (threading.current_thread().name, project_uuid))

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
        logger.info("Exit summary index worker process")
        os._exit(0)

    def signal_term_handler(self, signal, frame):
        self.clear()

    def set_signal(self):
        signal.signal(signal.SIGTERM, self.signal_term_handler)


def start():
    summary_index_task_worker = SummaryIndexTaskWorker()
    logger.info("Summary index task worker process initialized.")
    try:
        summary_index_task_worker.start()
    except Exception as e:
        logger.exception(e)
        summary_index_task_worker.clear()
        os._exit(0)

    while True:
        time.sleep(2)


def main():
    LogConfigurator(config.LOG_LEVEL, config.LOG_FILE)
    start()


if __name__ == "__main__":
    main()
