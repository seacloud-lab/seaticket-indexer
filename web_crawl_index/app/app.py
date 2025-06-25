# Install the correct reactor before importing any twisted modules
from scrapy.utils.reactor import install_reactor
# Use AsyncioSelectorReactor instead of SelectReactor
install_reactor('twisted.internet.asyncioreactor.AsyncioSelectorReactor')

from twisted.internet import reactor

from web_crawl_index.server.web_crawl_server import WebCrawlServer
from web_crawl_index.crawler.tasks.web_crawl_task_worker import WebCrawlTaskWorker


class WebCrawlIndexApp(object):
    def __init__(self, ):
        self.web_crawl_task_worker = WebCrawlTaskWorker()
        self.web_crawl_server = WebCrawlServer()
        reactor.suggestThreadPoolSize(10)

        # self.start_reactor()

    def start_reactor(self):
        from threading import Thread
        # Thread(target=self.run_reactor, name='reactor_thread', daemon=True).start()
        Thread(target=reactor.run, name='reactor_thread', daemon=True, kwargs={'installSignalHandlers': False}).start()

    # def run_reactor(self):
    #     reactor.run(installSignalHandlers=False)

    def serve_forever(self):
        self.web_crawl_task_worker.start()
        self.web_crawl_server.start()
        reactor.run(installSignalHandlers=False)
