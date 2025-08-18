import time
import logging
import json
import threading
import os

from redis.exceptions import ConnectionError as NoMQAvailable, ResponseError, TimeoutError

from seaqa_indexer import config
from seaqa_indexer.app.log import LogConfigurator
from seaqa_indexer.syncer.discourse_syncer.discourse_sync_manager import DiscourseSyncManager
from seaqa_indexer.mq import get_mq
from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.utils.constants import CrawlStatus


logger = logging.getLogger(__name__)


class DiscourseSyncTaskWorker(object):
    """ The handler for redis message queue
    """

    def __init__(self):
        self.db_session = init_db_session_class()
        self.discourse_sync_manager = DiscourseSyncManager()
        self.should_stop = threading.Event()
        self.worker_num = 3
        self.update_discourse_interval = 7 * 24 * 60 * 60

        self.LOCK_TIMEOUT = 1800  # 30 minutes
        self.REFRESH_INTERVAL = 600  # 10 minutes
        self.locked_keys = set()

        self.mq = get_mq(config.REDIS_HOST, config.REDIS_PORT, config.REDIS_PASSWORD)

        self.set_signal()
        self.worker_list = []

    def _get_discourse_lock_key(self, connection_id):
        return 'discourse_' + str(connection_id)

    def work_task_handler(self, connection_id):
        # Python cannot kill threads, so stop it generate more locked key.
        if not self.should_stop.is_set():
            # set key-value if does not exist which will expire 30 minutes later
            if self.mq.set(self._get_discourse_lock_key(connection_id), time.time(),
                      ex=self.LOCK_TIMEOUT, nx=True):
                # get lock
                logger.info('%s start discourse sync connection: %s' % (self.tname, connection_id))
                lock_key = self._get_discourse_lock_key(connection_id)
                self.locked_keys.add(lock_key)

                self.sync_discourse(connection_id)
                try:
                    self.locked_keys.remove(lock_key)
                except KeyError:
                    logger.error("%s is already removed. SHOULD NOT HAPPEN!" % lock_key)
                self.mq.delete(lock_key)
                logger.info(
                    "%s Finish discourse sync connection: %s, delete redis lock %s" % (self.tname, connection_id, lock_key))
            else:
                # the connection is updated by other thread, push back to the queue
                self.add_to_undo_task(connection_id)

    def sync_discourse(self, connection_id):
        project_connection = self.discourse_sync_manager.get_project_connection_by_id(connection_id)
        if not project_connection:
            logger.warning('connection: %s not found', connection_id)
            return
        site_status = json.loads(project_connection.status)
        last_sync_status = site_status.get('site_status')
        if last_sync_status == CrawlStatus.CRAWLING:
            logger.warning('connection: %s is syncing', connection_id)
            return
        try:
            self.discourse_sync_manager.sync_discourse(project_connection)
        except Exception as e:
            logger.exception('%s discourse sync connection %s error: %s' % (self.tname, connection_id, e))
            site_status['last_sync_status'] = CrawlStatus.FAILED
            self.discourse_sync_manager.update_status(connection_id, json.dumps(site_status))

    def add_to_undo_task(self, connection_id):
        """Push task back to the end of the queue.
        """
        # avoid get the same task repeatly
        time.sleep(0.5)

        msg_content = {
            'connection_id': connection_id,
        }
        self.mq.lpush('discourse_sync_task', json.dumps(msg_content))
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
        logger.info("Exit discourse sync worker process")
        os._exit(0)

    def signal_term_handler(self, signal, frame):
        self.clear()

    def set_signal(self):
        import signal
        signal.signal(signal.SIGTERM, self.signal_term_handler)

    @property
    def tname(self):
        return threading.current_thread().name

    def start(self):
        if not self.mq:
            return
        for i in range(int(self.worker_num)):
            t = threading.Thread(target=self.discourse_sync_handler, name='discourse_sync_handler_thread_' + str(i), daemon=True)
            t.start()
        threading.Thread(target=self.cron_sync_discourse_forums, name='cron_sync_discourse_forums', daemon=True).start()
        threading.Thread(target=self.refresh_lock, name='refresh_thread', daemon=True).start()

    def cron_sync_discourse_forums(self):
        logger.info('Start to sync discourse forum timer, interval = %s sec', self.update_discourse_interval)
        while not self.should_stop.is_set():
            self.should_stop.wait(self.update_discourse_interval)
            if not self.should_stop.is_set():
                try:
                    self.discourse_sync_manager.update_discourse_forums()
                except Exception as e:
                    logger.exception('cron sync discourse error: %s', e)

                logger.info('finish cron sync discourse forums')

    def discourse_sync_handler(self):
        logger.info('%s: discourse sync handler is running.' % self.tname)
        try:
            while not self.should_stop.is_set():
                try:
                    element = self.mq.brpop('discourse_sync_task', timeout=30)
                    if element is not None:
                        key, value = element
                        try:
                            data = json.loads(value)
                        except:
                            data = None
                        if not data:
                            logger.warning('discourse_sync_task: invalid.', element)
                        else:
                            connection_id = data.get('connection_id')
                            self.work_task_handler(connection_id)
                except (ResponseError, NoMQAvailable, TimeoutError) as e:
                    logger.error('The connection to the redis server failed: %s' % e)
        except Exception as e:
            logger.error('%s discourse sync connection Error' % self.tname)
            logger.error(e, exc_info=True)
            # prevent case that redis break at program running.
            time.sleep(0.3)

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
    discourse_sync_task_worker = DiscourseSyncTaskWorker()
    logger.info("discourse sync task worker process initialized.")
    try:
        discourse_sync_task_worker.start()
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
