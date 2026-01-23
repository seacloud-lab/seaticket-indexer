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


logger = logging.getLogger(__name__)


class ConnectionSyncTaskWorker(object):
    """ The handler for redis message queue
    """

    def __init__(self):
        self.db_session_class = init_db_session_class()
        self.should_stop = threading.Event()
        self.worker_num = 3

        self.LOCK_TIMEOUT = 1800  # 30 minutes

        self.mq = get_mq(config.REDIS_HOST, config.REDIS_PORT, config.REDIS_PASSWORD)
        self.seaqa_db = SeaqaDB(self.db_session_class)

        self.set_signal()
        self.worker_list = []

    @property
    def tname(self):
        return threading.current_thread().name

    def start(self):
        if not self.mq:
            return

        for i in range(int(self.worker_num)):
            t = threading.Thread(target=self.connection_sync_handler, name='sync_handler_thread_' + str(i), daemon=True)
            t.start()

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
        logger.info('%s start sync connection: %s' % (self.tname, connection_id))

        try:
            sync_connection(connection_id, self.seaqa_db, self.mq, self.db_session_class)
        except Exception as e:
            logger.exception('%s sync connection %s error: %s' % (self.tname, connection_id, e))
        logger.info("%s Finish sync connection: %s" % (self.tname, connection_id))

    def signal_term_handler(self, signal, frame):
        logger.info("Exit connection sync worker process")
        os._exit(0)

    def set_signal(self):
        import signal
        signal.signal(signal.SIGTERM, self.signal_term_handler)


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
