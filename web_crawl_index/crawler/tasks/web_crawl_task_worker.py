import json
import logging
import time
from threading import Event
import threading
from redis.exceptions import ConnectionError as NoMQAvailable, ResponseError, TimeoutError


from web_crawl_index.db import init_db_session_class
from web_crawl_index.mq import get_mq
from web_crawl_index.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD

from web_crawl_index.crawler.web_crawl_index_manager import WebCrawlIndexManager

logger = logging.getLogger(__name__)


class WebCrawlTaskWorker:
    """Manage the scheduled updates of site content"""

    def __init__(self):
        self.db_session = init_db_session_class()
        self.web_crawl_index_manager = WebCrawlIndexManager()
        self.update_indexes_interval = 7 * 24 * 60 * 60
        self.should_stop = Event()
        self.worker_num = 3

        self.mq = get_mq(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)

    @property
    def tname(self):
        return threading.current_thread().name

    def start(self):

        if not self.mq:
            return
        for i in range(int(self.worker_num)):
            threading.Thread(target=self.web_crawl_handler, name='web_crawl_handler_thread_' + str(i), daemon=True).start()

        threading.Thread(target=self.cron_update_web_indexes_timer, name='timer', daemon=True).start()

    def web_crawl_handler(self):
        logger.info('%s starting web crawl work' % self.tname)
        try:
            while not self.should_stop.isSet():
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
        try:
            self.web_crawl_index_manager.crawl_site(site_id)
        except Exception as e:
            logger.exception('crawl site: %s failed, error: %s', site_id, e)

    def cron_update_web_indexes_timer(self):
        logger.info('Start to update web crawl, interval = %s sec', self.update_indexes_interval)
        while not self.should_stop.is_set():
            self.should_stop.wait(self.update_indexes_interval)
            if not self.should_stop.is_set():
                try:
                    self.web_crawl_index_manager.update_sites_indexes()
                except Exception as e:
                    logger.exception('cron update site index error: %s', e)
                logger.info('finish updating crawl web...')

