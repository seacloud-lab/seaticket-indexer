import logging
from threading import Thread, Event
from web_crawl_index.crawler.site_index_manager import SiteIndexManager


logger = logging.getLogger(__name__)


class SiteIndexUpdater(object):
    def __init__(self, site_index_manager):
        self._enabled = True
        self._site_index_manager = site_index_manager
        self._interval = 30 * 60

    def is_enabled(self):
        return self._enabled

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start site index updater: it is not enabled!')
            return

        logging.info('Start to update site index, interval = %s sec', self._interval)
        SiteIndexUpdaterTimer(
            self._site_index_manager,
            self._interval
        ).start()


class SiteIndexUpdaterTimer(Thread):
    def __init__(self, site_index_manager, interval):
        super(SiteIndexUpdaterTimer, self).__init__()
        self.site_index_manager = site_index_manager
        self.interval = interval
        self.finished = Event()

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self.interval)
            if not self.finished.is_set():
                logger.info('starts to update site index...')
                try:
                    self.site_index_manager.update_site_indexes()
                except Exception as e:
                    logger.exception('periodical update site index error: %s', e)

                logger.info('finish updating site index...')

    def cancel(self):
        self.finished.set()
