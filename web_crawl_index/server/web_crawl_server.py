from threading import Thread
from waitress import create_server

from web_crawl_index.config import HOST, PORT
from web_crawl_index.server.apis import application

from web_crawl_index.discourse.discourse_manager import DiscourseManager


class WebCrawlServer(Thread):
    def __init__(self, web_crawl_manager, site_index_manager):
        Thread.__init__(self)
        application.web_crawl_manager = web_crawl_manager
        application.site_index_manager = site_index_manager
        
        application.discourse_manager = DiscourseManager(application)
        application.discourse_manager.start()
        
        self.server = create_server(application, host=HOST, port=PORT)

    def run(self):
        self.server.run()
