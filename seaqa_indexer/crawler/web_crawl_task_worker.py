import os
import logging
import threading
import signal

# Install the correct reactor before importing any twisted modules
from scrapy.utils.reactor import install_reactor
# Use AsyncioSelectorReactor instead of SelectReactor
install_reactor('twisted.internet.asyncioreactor.AsyncioSelectorReactor')

from twisted.internet import reactor

from seaqa_indexer import config
from seaqa_indexer.app.log import LogConfigurator
from seaqa_indexer.workers.base_task_worker import BaseTaskWorker, run_worker
from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.crawler.crawl_web import craw_web


logger = logging.getLogger(__name__)


class WebCrawlTaskWorker(BaseTaskWorker):
    """The handler for redis message queue - web crawl tasks"""

    @property
    def worker_num(self) -> int:
        return 3

    def get_queue_name(self) -> str:
        return 'web_crawl_task'

    def get_worker_name(self) -> str:
        return 'web_crawl_handler'

    def init_dependencies(self):
        self.db_session_class = init_db_session_class()
        reactor.suggestThreadPoolSize(10)

    def on_start(self):
        """start twisted reactor"""
        threading.Thread(
            target=reactor.run,
            name='reactor_thread',
            daemon=True,
            kwargs={'installSignalHandlers': False}
        ).start()

    def process_message(self, data: dict):
        connection_id = data.get('site_id')
        self.crawl_site(connection_id)

    def crawl_site(self, connection_id):
        try:
            craw_web(connection_id, self.db_session_class, self.mq)
        except Exception as e:
            logger.exception('crawl site: %s failed, error: %s', connection_id, e)

    def signal_term_handler(self, signal, frame):
        logger.info("Exit web_crawl task worker process")
        os._exit(0)

    def set_signal(self):
        # TODO: look like python will add signal to queue when cpu exec c extension code,
        # and will call signal callback method after cpu exec python code
        # ref: https://docs.python.org/2/library/signal.html
        signal.signal(signal.SIGTERM, self.signal_term_handler)


def main():
    os.environ.setdefault('SCRAPY_SETTINGS_MODULE', 'seaqa_indexer.crawler.settings')
    run_worker(WebCrawlTaskWorker)


if __name__ == "__main__":
    main()
