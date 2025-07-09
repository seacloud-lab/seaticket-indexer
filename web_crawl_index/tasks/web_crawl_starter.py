# coding: UTF-8
import logging
from threading import Thread, Event

from web_crawl_index.config import LOG_FILE, LOG_LEVEL
from web_crawl_index.utils import get_python_executable, run


__all__ = [
    'WebCrawlStarter',
]


class WebCrawlStarter(object):
    def __init__(self):
        self._enabled = True
        self._logfile = LOG_FILE
        self._loglevel = LOG_LEVEL

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start web crawl starter: it is not enabled!')
            return

        WebCrawl(self._logfile, self._loglevel).start()
        logging.info('web crawl starter is started')

    def is_enabled(self):
        return self._enabled


class WebCrawl(Thread):
    def __init__(self, logfile, loglevel):
        Thread.__init__(self)
        self._logfile = logfile
        self._loglevel = loglevel
        self.finished = Event()

    def run(self):
        try:
            cmd = [
                get_python_executable(),
                '-m', 'web_crawl_index.crawler.web_crawl_task_worker'
            ]
            run(cmd, )
        except Exception as e:
            logging.exception('error when start web_crawl_starter: %s', e)

    def cancel(self):
        self.finished.set()
