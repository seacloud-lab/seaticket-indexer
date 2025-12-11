import os
import time
import logging
import json
import threading
import signal
from redis.exceptions import ConnectionError as NoMQAvailable, ResponseError, TimeoutError

# Install the correct reactor before importing any twisted modules
from scrapy.utils.reactor import install_reactor
# Use AsyncioSelectorReactor instead of SelectReactor
install_reactor('twisted.internet.asyncioreactor.AsyncioSelectorReactor')

from twisted.internet import reactor

from seaqa_indexer import config
from seaqa_indexer.app.log import LogConfigurator
from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.crawler.crawl_web import craw_web
from seaqa_indexer.mq import get_mq


logger = logging.getLogger(__name__)


class WebCrawlTaskWorker(object):
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
        reactor.suggestThreadPoolSize(10)
        self.set_signal()
        self.worker_list = []

    @property
    def tname(self):
        return threading.current_thread().name

    def _get_lock_key(self, connection_id):
        return 'web_crawl_' + str(connection_id)

    def clear_worker(self):
        for th in self.worker_list:
            th.join()
        logger.info("All web crawl handler threads has stopped.")

    def start(self):
        if not self.mq:
            return
        for i in range(int(self.worker_num)):
            t = threading.Thread(target=self.web_crawl_handler, name='web_crawl_handler_thread_' + str(i), daemon=True)
            t.start()

        threading.Thread(target=reactor.run, name='reactor_thread', daemon=True, kwargs={'installSignalHandlers': False}).start()
        threading.Thread(target=self.refresh_lock, name='refresh_thread', daemon=True).start()

    def web_crawl_handler(self):
        logger.info('%s starting web crawl work' % self.tname)
        try:
            while not self.should_stop.is_set():
                try:
                    res = self.mq.brpop('web_crawl_task', timeout=30)
                    if res is not None:
                        key, value = res
                        try:
                            data = json.loads(value)
                        except:
                            data = None

                        if not data:
                            logger.warning('web_crawl_task: invalid.', res)
                        else:
                            connection_id = data.get('site_id')
                            self.work_task_handler(connection_id)
                except (ResponseError, NoMQAvailable, TimeoutError) as e:
                    logger.error('The connection to the redis server failed: %s' % e)
        except Exception as e:
            logger.error('%s Crawl site Error' % self.tname)
            logger.error(e, exc_info=True)
            # prevent case that redis break at program running.
            time.sleep(0.3)

    def work_task_handler(self, connection_id):
        # Python cannot kill threads, so stop it generate more locked key.
        if not self.should_stop.is_set():
            # set key-value if does not exist which will expire 30 minutes later
            if self.mq.set(self._get_lock_key(connection_id), time.time(), ex=self.LOCK_TIMEOUT, nx=True):
                # get lock
                logger.info('%s start sync connection: %s' % (self.tname, connection_id))
                lock_key = self._get_lock_key(connection_id)
                self.locked_keys.add(lock_key)
                self.crawl_site(connection_id, lock_key)

    def crawl_site(self, connection_id, lock_key):
        try:
            craw_web(connection_id, self.db_session_class, self.mq, self.delete_lock, lock_key)
        except Exception as e:
            self.delete_lock(lock_key)
            logger.exception('crawl site: %s failed, error: %s', connection_id, e)

    def delete_lock(self, lock_key):
        try:
            self.locked_keys.remove(lock_key)
        except KeyError:
            logger.error("%s is already removed. SHOULD NOT HAPPEN!" % lock_key)
        self.mq.delete(lock_key)
        logger.info("%s Finish craw connection, delete redis lock %s" % (self.tname, lock_key))

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
        logger.info("Exit connection web_crawl worker process.")
        os._exit(0)

    def signal_term_handler(self, signal, frame):
        self.clear()

    def set_signal(self):
        # TODO: look like python will add signal to queue when cpu exec c extension code,
        # and will call signal callback method after cpu exec python code
        # ref: https://docs.python.org/2/library/signal.html
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
                    logger.debug('%s Refresh lock [%s] timeout from %s to %s' % (self.tname, lock, ttl, new_ttl))
                time.sleep(self.REFRESH_INTERVAL)
            except Exception as e:
                logger.exception(e)
                time.sleep(1)


def start():
    web_crawl_task_worker = WebCrawlTaskWorker()
    logger.info("web crawl task worker process initialized.")
    try:
        web_crawl_task_worker.start()
    except Exception as e:
        logger.exception(e)
        os._exit(0)

    while True:
        # if main thread has been quit or join for subthread.
        # signal callback will never be  call.
        time.sleep(2)


def main():
    LogConfigurator(config.LOG_LEVEL, config.LOG_FILE)
    os.environ.setdefault('SCRAPY_SETTINGS_MODULE', 'seaqa_indexer.crawler.settings')
    start()


if __name__ == "__main__":
    main()
