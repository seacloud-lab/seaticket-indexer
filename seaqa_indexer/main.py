import config

from seaqa_indexer.app.log import LogConfigurator
from seaqa_indexer.app.app import WebCrawlIndexApp
from seaqa_indexer.app.signal_handler import set_signal


def main():
    LogConfigurator(config.LOG_LEVEL, config.LOG_FILE)
    set_signal()

    app = WebCrawlIndexApp()
    app.serve_forever()


if __name__ == '__main__':
    main()
