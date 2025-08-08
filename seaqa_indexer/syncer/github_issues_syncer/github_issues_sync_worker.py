import os
import time
import logging
import json
import threading
import signal
from redis.exceptions import ConnectionError as NoMQAvailable, ResponseError, TimeoutError

# Install the correct reactor before importing any twisted modules
from scrapy.utils.reactor import install_reactor

from seaqa_indexer.syncer.github_issues_syncer.github_issues_sync_manager import GithubIssuesSyncManager
from seaqa_indexer.utils.constants import CrawlStatus

# Use AsyncioSelectorReactor instead of SelectReactor
install_reactor('twisted.internet.asyncioreactor.AsyncioSelectorReactor')

from twisted.internet import reactor

from seaqa_indexer import config
from seaqa_indexer.app.log import LogConfigurator


from seaqa_indexer.mq import get_mq
from seaqa_indexer.db import init_db_session_class

logger = logging.getLogger(__name__)


class GithubIssuesSyncTaskWorker(object):
    """ The handler for redis message queue
    """

    def __init__(self):
        self.db_session = init_db_session_class()
        self.issues_sync_manager = GithubIssuesSyncManager()
        self.should_stop = threading.Event()
        self.worker_num = 3
        self.sync_issues_interval = 7 * 24 * 60 * 60

        self.mq = get_mq(config.REDIS_HOST, config.REDIS_PORT, config.REDIS_PASSWORD)
        reactor.suggestThreadPoolSize(10)

        self.set_signal()
        self.worker_list = []

    @property
    def tname(self):
        return threading.current_thread().name

    def clear_worker(self):
        for th in self.worker_list:
            th.join()
        logger.info("All issues sync handler threads has stopped.")

    def start(self):
        if not self.mq:
            return
        for i in range(int(self.worker_num)):
            t = threading.Thread(target=self.issues_sync_handler, name='issues_sync_handler_thread_' + str(i), daemon=True)
            t.start()

        threading.Thread(target=self.cron_sync_issues, name='cron_sync_issues_thread', daemon=True).start()
        threading.Thread(target=reactor.run, name='reactor_thread', daemon=True, kwargs={'installSignalHandlers': False}).start()

    def issues_sync_handler(self):
        logger.info('%s starting issues sync work' % self.tname)
        try:
            while not self.should_stop.is_set():
                try:
                    res = self.mq.brpop('issues_sync_task', timeout=30)
                    if res is not None:
                        key, value = res
                        try:
                            data = json.loads(value)
                        except:
                            data = None

                        if not data:
                            logger.warning('issues_sync_task: invalid.', res)
                        else:
                            issues_sync_id = data.get('issues_sync_id')
                            self.issues_sync(issues_sync_id)
                except (ResponseError, NoMQAvailable, TimeoutError) as e:
                    logger.error('The connection to the redis server failed: %s' % e)
        except Exception as e:
            logger.error('%s Sync issues Error' % self.tname)
            logger.error(e, exc_info=True)
            # prevent case that redis break at program running.
            time.sleep(0.3)

    def issues_sync(self, issues_sync_id):
        try:
            self.issues_sync_manager.issues_sync(issues_sync_id)
        except Exception as e:
            logger.exception('sync issues: %s failed, error: %s', issues_sync_id, e)
            self.issues_sync_manager.update_issues_sync_status(issues_sync_id, CrawlStatus.FAILED)

    def cron_sync_issues(self):
        logger.info('Start to sync issues timer, interval = %s sec', self.sync_issues_interval)
        while not self.should_stop.is_set():
            self.should_stop.wait(self.sync_issues_interval)
            if not self.should_stop.is_set():
                try:
                    self.issues_sync_manager.update_issues_sync()
                    pass
                except Exception as e:
                    logger.exception('cron sync issues error: %s', e)

                logger.info('finish cron sync issues...')

    def signal_term_handler(self, signal, frame):
        os._exit(0)

    def set_signal(self):
        # TODO: look like python will add signal to queue when cpu exec c extension code,
        # and will call signal callback method after cpu exec python code
        # ref: https://docs.python.org/2/library/signal.html
        signal.signal(signal.SIGTERM, self.signal_term_handler)


def start():

    issues_sync_worker = GithubIssuesSyncTaskWorker()
    logger.info("issues sync task worker process initialized.")
    try:
        issues_sync_worker.start()
    except Exception as e:
        logger.exception(e)
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
