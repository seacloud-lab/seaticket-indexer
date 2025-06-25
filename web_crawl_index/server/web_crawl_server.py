from threading import Thread
from waitress import create_server

from web_crawl_index.config import HOST, PORT
from web_crawl_index.server.apis import application
from web_crawl_index.crawler.web_crawl_index_manager import WebCrawlIndexManager


class WebCrawlServer(Thread):
    def __init__(self):
        Thread.__init__(self)
        web_crawl_index_manager = WebCrawlIndexManager()
        application.web_crawl_index_manager = web_crawl_index_manager
        self.server = create_server(application, host=HOST, port=PORT)

    def run(self):
        self.server.run()
