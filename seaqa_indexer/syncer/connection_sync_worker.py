import time
import logging
import json
import threading
import os

from redis.exceptions import ConnectionError as NoMQAvailable, ResponseError, TimeoutError

from seaqa_indexer import config
from seaqa_indexer.app.log import LogConfigurator
from seaqa_indexer.mq import get_mq
from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.syncer.sync_connection import sync_connection
from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.utils.sync_logger import SyncLogger
from seaqa_indexer.utils.constants import ConnectionType


logger = logging.getLogger(__name__)

NEED_RECOVER_SYNC_TYPES = [ConnectionType.GITHUB_ISSUE.value, ConnectionType.DISCOURSE_FORUM.value]


class ConnectionSyncTaskWorker(object):
    """ The handler for redis message queue
    """

    def __init__(self):
        self.db_session_class = init_db_session_class()
        self.should_stop = threading.Event()
        self.worker_num = 3

        self.LOCK_TIMEOUT = 1800  # 30 minutes
        self.REFRESH_INTERVAL = 600  # 10 minutes
        self.locked_keys = set()

        self.mq = get_mq(config.REDIS_HOST, config.REDIS_PORT, config.REDIS_PASSWORD)
        self.seaqa_db = SeaqaDB(self.db_session_class)

        self.set_signal()
        self.worker_list = []

    def _get_sync_lock_key(self, connection_id):
        return 'sync_' + str(connection_id)

    @property
    def tname(self):
        return threading.current_thread().name

    def start(self):
        if not self.mq:
            return

        self.recover_interrupted_tasks()

        for i in range(int(self.worker_num)):
            t = threading.Thread(target=self.connection_sync_handler, name='sync_handler_thread_' + str(i), daemon=True)
            t.start()
        threading.Thread(target=self.refresh_lock, name='refresh_thread', daemon=True).start()

    def recover_interrupted_tasks(self):
        logger.info("Start to recover interrupted sync tasks...")
        try:
            connections = self.seaqa_db.get_connections_to_recover(NEED_RECOVER_SYNC_TYPES)
            for connection in connections:
                connection_id = connection.id

                logger.info(f"Add recovering sync task for connection {connection_id}")
                self.add_to_undo_task(connection_id)

        except Exception as e:
            logger.exception(f"Error during task recovery: {e}")

    def connection_sync_handler(self):
        logger.info('%s: sync handler is running.' % self.tname)
        try:
            while not self.should_stop.is_set():
                try:
                    element = self.mq.brpop('sync_task', timeout=30)
                    if element is not None:
                        key, value = element
                        try:
                            data = json.loads(value)
                        except:
                            data = None
                        if not data:
                            logger.warning('sync_task: invalid.', element)
                        else:
                            connection_id = data.get('connection_id')
                            self.work_task_handler(connection_id)
                except (ResponseError, NoMQAvailable, TimeoutError) as e:
                    logger.error('The connection to the redis server failed: %s' % e)
        except Exception as e:
            logger.error('%s connection sync connection Error' % self.tname)
            logger.error(e, exc_info=True)
            # prevent case that redis break at program running.
            time.sleep(0.3)

    def work_task_handler(self, connection_id):
        # Python cannot kill threads, so stop it generate more locked key.
        if not self.should_stop.is_set():
            # set key-value if does not exist which will expire 30 minutes later
            if self.mq.set(self._get_sync_lock_key(connection_id), time.time(),
                      ex=self.LOCK_TIMEOUT, nx=True):
                # get lock
                logger.info('%s start sync connection: %s' % (self.tname, connection_id))
                lock_key = self._get_sync_lock_key(connection_id)
                self.locked_keys.add(lock_key)

                self.sync(connection_id)
                try:
                    self.locked_keys.remove(lock_key)
                except KeyError:
                    logger.error("%s is already removed. SHOULD NOT HAPPEN!" % lock_key)
                self.mq.delete(lock_key)
                logger.info(
                    "%s Finish sync connection: %s, delete redis lock %s" % (self.tname, connection_id, lock_key))
            else:
                # the connection is updated by other thread, push back to the queue
                self.add_to_undo_task(connection_id)

    def sync(self, connection_id):
        try:
            task_sync_logger = SyncLogger(self.db_session_class)
            sync_connection(connection_id, self.seaqa_db, self.mq, task_sync_logger)
        except Exception as e:
            logger.exception('%s sync connection %s error: %s' % (self.tname, connection_id, e))

    def add_to_undo_task(self, connection_id):
        """Push task back to the front of the queue.
        """
        # avoid get the same task repeatly
        time.sleep(0.5)

        msg_content = {
            'connection_id': connection_id,
        }
        self.mq.lpush('sync_task', json.dumps(msg_content))
        logger.debug('%s push back task connection_id: %s, to the queue' %
                     (threading.current_thread().name, connection_id))

    def clear(self):
        if not self.mq:
            return
        self.should_stop.set()
        # if a thread just lock key, wait to add the lock to the list.
        time.sleep(1)
        # del redis locked key
        for key in self.locked_keys:
            self.mq.delete(key)
            logger.info("redis lock key %s has been deleted", key)
        # sys.exit
        logger.info("Exit connection sync worker process")
        os._exit(0)

    def signal_term_handler(self, signal, frame):
        self.clear()

    def set_signal(self):
        import signal
        signal.signal(signal.SIGTERM, self.signal_term_handler)

    def refresh_lock(self):
        logger.info('%s Starting refresh locks', self.tname)
        while not self.should_stop.is_set():
            try:
                # workaround for the RuntimeError: Set changed size during iteration
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

def start_worker():
    connection_sync_task_worker = ConnectionSyncTaskWorker()
    logger.info("sync task worker process initialized.")
    try:
        connection_sync_task_worker.start()
    except Exception as e:
        logger.exception(e)
        os._exit(0)

    while True:
        time.sleep(2)


def main():
    LogConfigurator(config.LOG_LEVEL, config.LOG_FILE)
    start_worker()


if __name__ == "__main__":
    main()
