from threading import Thread
from waitress import create_server

from seaqa_indexer.config import HOST, PORT
from seaqa_indexer.server.apis import application


class HttpServer(Thread):
    def __init__(self, app):
        Thread.__init__(self)
        application.app = app
        self.server = create_server(application, host=HOST, port=PORT)

    def run(self):
        self.server.run()
