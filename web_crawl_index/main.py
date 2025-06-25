import os
import config

from web_crawl_index.app.log import LogConfigurator
from web_crawl_index.app.app import WebCrawlIndexApp


def main():
    os.environ.setdefault('SCRAPY_SETTINGS_MODULE', 'web_crawl_index.crawler.settings')

    # Initialize logger with default config values
    LogConfigurator(config.LOG_LEVEL, config.LOG_FILE)

    app = WebCrawlIndexApp()
    app.serve_forever()


if __name__ == '__main__':
    main()
