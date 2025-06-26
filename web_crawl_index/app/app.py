from web_crawl_index.server.web_crawl_server import WebCrawlServer
from web_crawl_index.tasks.web_crawl_index_worker import WebCrawlIndexWorker


class WebCrawlIndexApp(object):
    def __init__(self, ):
        self.web_crawl_index_task_worker = WebCrawlIndexWorker()
        self.web_crawl_server = WebCrawlServer()

    def serve_forever(self):
        self.web_crawl_index_task_worker.start()
        self.web_crawl_server.start()
