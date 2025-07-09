from web_crawl_index.server.web_crawl_server import WebCrawlServer
from web_crawl_index.tasks.web_crawl_starter import WebCrawlStarter
from web_crawl_index.crawler.web_crawl_manager import WebCrawlManager
from web_crawl_index.seasearch.index_manager import IndexManager
from web_crawl_index.tasks.index_worker_starter import IndexWorkerStarter


class WebCrawlIndexApp(object):
    def __init__(self, ):
        self.web_crawl_manager = WebCrawlManager()
        self.index_manager = IndexManager()

        self.web_crawl_task_starter = WebCrawlStarter()
        self.index_worker_starter = IndexWorkerStarter()
        self.web_crawl_server = WebCrawlServer(self.web_crawl_manager, self.index_manager)

    def serve_forever(self):
        self.web_crawl_task_starter.start()
        self.index_worker_starter.start()
        self.web_crawl_server.start()
