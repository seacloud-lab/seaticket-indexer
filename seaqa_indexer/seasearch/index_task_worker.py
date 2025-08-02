import os
import time
import logging
import json
import threading
import signal
from redis.exceptions import ConnectionError as NoMQAvailable, ResponseError, TimeoutError


from seaqa_indexer import config
from seaqa_indexer.app.log import LogConfigurator
from seaqa_indexer.seasearch.index_manager import IndexManager

from seaqa_indexer.mq import get_mq
from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.utils import time_difference_to_midnight


logger = logging.getLogger(__name__)


class IndexTaskWorker(object):
    """ The handler for redis message queue
    """

    def __init__(self):
        self.db_session = init_db_session_class()
        self.index_manager = IndexManager()
        self.should_stop = threading.Event()
        self.worker_num = 3

        self.LOCK_TIMEOUT = 1800  # 30 minutes
        self.REFRESH_INTERVAL = 600
        self.locked_keys = set()

        self.mq = get_mq(config.REDIS_HOST, config.REDIS_PORT, config.REDIS_PASSWORD)

        self.set_signal()
        self.worker_list = []

    @property
    def tname(self):
        return threading.current_thread().name

    def _get_index_lock_key(self, connection_id):
        return 'index_' + str(connection_id)

    def clear_worker(self):
        for th in self.worker_list:
            th.join()
        logger.info("All index task threads has stopped.")

    def start(self):
        if not self.mq:
            return
        for i in range(int(self.worker_num)):
            t = threading.Thread(target=self.index_task_worker, name='index_task_worker_thread_' + str(i), daemon=True)
            t.start()

        threading.Thread(target=self.cron_update_indexes, name='cron_update_indexes_thread', daemon=True).start()
        threading.Thread(target=self.refresh_lock, name='refresh_thread', daemon=True).start()

    def index_task_worker(self):
        logger.info('%s starting index task worker' % self.tname)
        try:
            while not self.should_stop.is_set():
                try:
                    res = self.mq.brpop('index_task', timeout=30)
                    if res is not None:
                        key, value = res
                        try:
                            data = json.loads(value)
                        except:
                            data = None
                        if not data:
                            logger.warning('index_task: invalid.', res)
                        else:
                            connection_id = data.get('connection_id')
                            self.work_task_handler(connection_id)
                except (ResponseError, NoMQAvailable, TimeoutError) as e:
                    logger.error('The connection to the redis server failed: %s' % e)
        except Exception as e:
            logger.error('%s index project connection Error' % self.tname)
            logger.error(e, exc_info=True)
            # prevent case that redis break at program running.
            time.sleep(0.3)

    def work_task_handler(self, connection_id):
        # Python cannot kill threads, so stop it generate more locked key.
        if not self.should_stop.is_set():
            # set key-value if does not exist which will expire 30 minutes later
            if self.mq.set(self._get_index_lock_key(connection_id), time.time(),
                      ex=self.LOCK_TIMEOUT, nx=True):
                # get lock
                logger.info('%s start index project connection: %s' % (self.tname, connection_id))
                lock_key = self._get_index_lock_key(connection_id)
                self.locked_keys.add(lock_key)
                self.update_index(connection_id)
                try:
                    self.locked_keys.remove(lock_key)
                except KeyError:
                    logger.error("%s is already removed. SHOULD NOT HAPPEN!" % lock_key)
                self.mq.delete(lock_key)
                logger.info(
                    "%s Finish index connection: %s, delete redis lock %s" % (self.tname, connection_id, lock_key))
            else:
                # the repo is updated by other thread, push back to the queue
                self.add_to_undo_task(connection_id)

    def update_index(self, connection_id):
        try:
            self.index_manager.update_index(connection_id)
        except Exception as e:
            logger.exception('update connection: %s index failed, error: %s', connection_id, e)

    def add_to_undo_task(self, connection_id):
        """Push task back to the end of the queue.
        """
        # avoid get the same task repeatly
        time.sleep(0.5)

        msg_content = {
            'connection_id': connection_id,
        }
        self.mq.lpush('index_task', json.dumps(msg_content))
        logger.info('%s push back task connection_id: %s, to the queue' %
                     (threading.current_thread().name, connection_id))

    def cron_update_indexes(self):
        logger.info('Start to update index timer, triggered at midnight')
        while not self.should_stop.is_set():

            now_to_midnight_second = time_difference_to_midnight()
            self.should_stop.wait(now_to_midnight_second)

            if not self.should_stop.is_set():
                logger.info('start to update indexes')
                try:
                    self.index_manager.update_indexes()
                except Exception as e:
                    logger.exception('cron update index error: %s', e)

                logger.info('put all indexes task into queue...')

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
        logger.info("Exit index worker process")
        os._exit(0)

    def signal_term_handler(self, signal, frame):
        self.clear()

    def set_signal(self):
        # TODO: look like python will add signal to queue when cpu exec c extension code,
        # and will call signal callback method after cpu exec python code
        # ref: https://docs.python.org/2/library/signal.html
        signal.signal(signal.SIGTERM, self.signal_term_handler)


def start():
    index_task_worker = IndexTaskWorker()
    logger.info("index task worker process initialized.")
    try:
        index_task_worker.start()
    except Exception as e:
        logger.exception(e)
        index_task_worker.clear()
        os._exit(0)

    while True:
        # if main thread has been quit or join for subthread.
        # signal callback will never be  call.
        time.sleep(2)


def main():
    LogConfigurator(config.LOG_LEVEL, config.LOG_FILE)
    start()


if __name__ == "__main__":
    main()
