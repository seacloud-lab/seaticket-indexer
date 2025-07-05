from web_crawl_index.server.web_crawl_server import WebCrawlServer
from web_crawl_index.tasks.web_crawl_starter import WebCrawlStarter
from web_crawl_index.tasks.site_index_updater import SiteIndexUpdater
from web_crawl_index.crawler.web_crawl_manager import WebCrawlManager
from web_crawl_index.crawler.site_index_manager import SiteIndexManager


class WebCrawlIndexApp(object):
    def __init__(self, ):
        self.web_crawl_manager = WebCrawlManager()
        self.site_index_manager = SiteIndexManager()

        self.web_crawl_task_starter = WebCrawlStarter()
        self.site_index_updater = SiteIndexUpdater(self.site_index_manager)
        self.web_crawl_server = WebCrawlServer(self.web_crawl_manager, self.site_index_manager)

    def serve_forever(self):
        self.web_crawl_task_starter.start()
        self.site_index_updater.start()
        self.web_crawl_server.start()
