import os

import config

from flask import Flask
from app.log_configurator import LogConfigurator
from models import db
from server.apis import api_bp
from crawler.crawler_manager import CrawlerManager

from threading import Thread
from waitress import serve


class BatchCrawlHttpServer(Thread):
    def __init__(self, app):
        Thread.__init__(self)
        self.app = app
        self.daemon = True

    def run(self):
        serve(self.app, host='0.0.0.0', port=8311)


class SeatableBatchCrawlApp:
    def __init__(self, config):
        self.config = config
        self.app = Flask(__name__)
        self.setup_app()


    def setup_app(self):
        """Configure Flask application"""
        # Configure database
        self.app.config['SQLALCHEMY_DATABASE_URI'] = self.config.DATABASE_URI
        self.app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
        
        # Initialize the database
        db.init_app(self.app)
        
        # Create database table
        with self.app.app_context():
            db.create_all()
        
        # Initialize the crawler manager
        self.app.crawler_manager = CrawlerManager(self.app, self.config.CRAWL_OUTPUT_DIR)
        
        # Register Blueprint
        self.app.register_blueprint(api_bp)
    
    def serve_forever(self):
        """Start Flask application"""
        # Create and start an HTTP server thread
        server = BatchCrawlHttpServer(self.app)
        server.start()
        
        try:
            # Keep the main thread running
            server.join()
        except KeyboardInterrupt:
            print("Server is shutting down...")

def main():
    # Configuration log
    os.environ.setdefault('SCRAPY_SETTINGS_MODULE', 'crawler.settings')

    # Initialize logger with default config values
    LogConfigurator()

    app = SeatableBatchCrawlApp(config)
    app.serve_forever()

if __name__ == '__main__':
    main()
