from threading import Thread
from waitress import create_server

from seaqa_indexer.config import HOST, PORT
from seaqa_indexer.server.apis import application


class HttpServer(Thread):
    def __init__(self, web_crawl_manager, index_manager, github_issues_sync_manager):
        Thread.__init__(self)
        application.web_crawl_manager = web_crawl_manager
        application.index_manager = index_manager
        application.github_issues_sync_manager = github_issues_sync_manager
        self.server = create_server(application, host=HOST, port=PORT)

    def run(self):
        self.server.run()
