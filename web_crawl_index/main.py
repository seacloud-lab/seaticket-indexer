import config

from web_crawl_index.app.log import LogConfigurator
from web_crawl_index.app.app import WebCrawlIndexApp
from web_crawl_index.app.signal_handler import set_signal


def main():
    LogConfigurator(config.LOG_LEVEL, config.LOG_FILE)
    set_signal()

    app = WebCrawlIndexApp()
    app.serve_forever()


if __name__ == '__main__':
    main()
