from seaqa_indexer.server.http_server import HttpServer
from seaqa_indexer.workers.web_crawl_worker_starter import WebCrawlWorkerStarter
from seaqa_indexer.crawler.web_crawl_manager import WebCrawlManager
from seaqa_indexer.seasearch.index_manager import IndexManager
from seaqa_indexer.workers.index_worker_starter import IndexWorkerStarter
from seaqa_indexer.tasks.project_connection_cleaner_starter import ProjectConnectionCleanerStarter


class WebCrawlIndexApp(object):
    def __init__(self, ):
        self.web_crawl_manager = WebCrawlManager()
        self.index_manager = IndexManager()

        self.web_crawl_worker_starter = WebCrawlWorkerStarter()
        self.index_worker_starter = IndexWorkerStarter()
        self.http_server = HttpServer(self.web_crawl_manager, self.index_manager)
        self.project_connection_cleaner_starter = ProjectConnectionCleanerStarter()

    def serve_forever(self):
        self.web_crawl_worker_starter.start()
        self.index_worker_starter.start()
        self.project_connection_cleaner_starter.start()
        self.http_server.start()
