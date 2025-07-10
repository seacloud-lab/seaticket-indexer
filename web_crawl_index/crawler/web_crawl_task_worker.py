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

from web_crawl_index import config
from web_crawl_index.app.log import LogConfigurator
from web_crawl_index.crawler.web_crawl_manager import WebCrawlManager

from web_crawl_index.mq import get_mq
from web_crawl_index.db import init_db_session_class
from web_crawl_index.utils.constants import CrawlStatus


logger = logging.getLogger(__name__)


class WebCrawlTaskWorker(object):
    """ The handler for redis message queue
    """
    
    def cron_cleanup_removed_urls_timer(self):
        """Periodically clean up removed URL records"""
        try:
            logger.info("Starting cleanup of old removed URLs...")
            
            # Get all completed sites
            with self.web_crawl_manager.db_session() as session:
                sql = "SELECT id FROM sites WHERE status = 'completed'"
                sites = session.execute(text(sql)).fetchall()
            
            total_cleaned = 0
            for site in sites:
                try:
                    cleaned = self.web_crawl_manager.cleanup_removed_urls_for_site(site.id, days_threshold=30)
                    total_cleaned += cleaned
                except Exception as e:
                    logger.error(f"Error cleaning site {site.id}: {str(e)}")
                    continue
            
            logger.info(f"Cleanup completed. Total removed URLs cleaned: {total_cleaned}")
            
        except Exception as e:
            logger.error(f"Error in cron_cleanup_removed_urls_timer: {str(e)}")
    
    def __init__(self):
        self.db_session = init_db_session_class()
        self.web_crawl_manager = WebCrawlManager()
        self.should_stop = threading.Event()
        self.worker_num = 3
        self.update_indexes_interval = 7 * 24 * 60 * 60
        self.site_urls_interval = 60 * 60 * 24
    
        self.mq = get_mq(config.REDIS_HOST, config.REDIS_PORT, config.REDIS_PASSWORD)
        reactor.suggestThreadPoolSize(10)
    
        self.set_signal()
        self.worker_list = []
    
        self.scheduler.add_job(
            self.cron_cleanup_removed_urls_timer,
            'interval',
            weeks=1,
            id='cleanup_removed_urls_timer'
        )

    @property
    def tname(self):
        return threading.current_thread().name

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

        threading.Thread(target=self.cron_update_web_indexes_timer, name='timer', daemon=True).start()

        threading.Thread(target=self.cron_process_site_urls_timer, name='site_urls_timer', daemon=True).start()
        threading.Thread(target=reactor.run, name='reactor_thread', daemon=True, kwargs={'installSignalHandlers': False}).start()

    def cron_process_site_urls_timer(self):
        logger.info('Start discovered URLs processing timer, interval = %s sec', self.site_urls_interval)
        while not self.should_stop.is_set():
            self.should_stop.wait(self.site_urls_interval)
            if not self.should_stop.is_set():
                try:
                    processed_count = self.web_crawl_manager.process_site_urls(limit=50)
                    if processed_count > 0:
                        logger.info(f'Processed {processed_count} discovered URLs')
                except Exception as e:
                    logger.exception('Error processing discovered URLs: %s', e)

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
                            site_id = data.get('site_id')
                            self.crawl_site(site_id)
                except (ResponseError, NoMQAvailable, TimeoutError) as e:
                    logger.error('The connection to the redis server failed: %s' % e)
        except Exception as e:
            logger.error('%s Crawl site Task Error' % self.tname)
            logger.error(e, exc_info=True)
            # prevent case that redis break at program running.
            time.sleep(0.3)

    def crawl_site(self, site_id):
        site = self.web_crawl_manager.get_site_by_id(site_id)
        if not site:
            logger.warning('site: %s not found', site_id)
            return
        if site.status == CrawlStatus.CRAWLING:
            logger.warning('site: %s is crawling', site_id)
            return
        self.web_crawl_manager.update_site_status(site_id, CrawlStatus.CRAWLING)
        try:
            self.web_crawl_manager.crawl_site(site)
        except Exception as e:
            logger.exception('crawl site: %s failed, error: %s', site_id, e)
            self.web_crawl_manager.update_site_status(site_id, CrawlStatus.FAILED)

    def cron_update_web_indexes_timer(self):
        logger.info('Start to update web crawl timer, interval = %s sec', self.update_indexes_interval)
        while not self.should_stop.is_set():
            self.should_stop.wait(self.update_indexes_interval)
            if not self.should_stop.is_set():
                try:
                    self.web_crawl_manager.update_sites_indexes()
                except Exception as e:
                    logger.exception('cron update site index error: %s', e)

                logger.info('finish updating crawl web...')

    def signal_term_handler(self, signal, frame):
        os._exit(0)

    def set_signal(self):
        # TODO: look like python will add signal to queue when cpu exec c extension code,
        # and will call signal callback method after cpu exec python code
        # ref: https://docs.python.org/2/library/signal.html
        signal.signal(signal.SIGTERM, self.signal_term_handler)


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
    os.environ.setdefault('SCRAPY_SETTINGS_MODULE', 'web_crawl_index.crawler.settings')
    start()


if __name__ == "__main__":
    main()
