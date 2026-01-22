import os
import time
import logging
import threading
import signal
import datetime

from seaqa_indexer import config
from seaqa_indexer.app.log import LogConfigurator
from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.utils.seasearch_api import SeaSearchAPI
from seaqa_indexer.handlers import get_handler_or_none


logger = logging.getLogger(__name__)


class DeletedRecordsCleaner(object):
    """
    Clean deleted records for recently active connections.
    """

    def __init__(self):
        self.db_session_class = init_db_session_class()
        self.seaqa_db = SeaqaDB(self.db_session_class)
        self.seadb_api = SeaDBAPI()
        self.seasearch_api = SeaSearchAPI(config.SEASEARCH_URL, config.SEASEARCH_TOKEN)

        self.should_stop = threading.Event()
        self.clear_interval = 24 * 60 * 60
        self.recent_days = 7  # Only check connections updated in the last 14 days
        self.delete_after_days = 14  # Only delete records that have been deleted for more than 14 days

        self.set_signal()

    @property
    def tname(self):
        return threading.current_thread().name

    def start(self):
        threading.Thread(
            target=self.cron_clean_deleted_records,
            name='cron_clean_deleted_records_thread',
            daemon=True
        ).start()

    def cron_clean_deleted_records(self):
        logger.info('Start deleted records cleaner timer')
        while not self.should_stop.is_set():
            self.should_stop.wait(self.clear_interval)

            if not self.should_stop.is_set():
                logger.info('Start to clean deleted records')
                try:
                    self.clean_deleted_records()
                except Exception as e:
                    logger.exception('Cron clean deleted records error: %s', e)

    def clean_deleted_records(self):
        connections = self.seaqa_db.get_recently_updated_connections(days=self.recent_days)
        logger.info('Found %d recently updated connections to check for deleted records', len(connections))

        for connection in connections:
            connection_id = connection.id
            connection_type = connection.type
            project_uuid = connection.project_uuid

            try:
                handler = get_handler_or_none(connection_type)
                if not handler:
                    logger.debug('Unsupported connection type for cleaning: %s', connection_type)
                    continue

                table_name = handler.gen_table_name(connection_id)
                cutoff_time = (datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=self.delete_after_days)).isoformat()

                total_deleted = handler.clean_deleted_records(
                    self.seadb_api, project_uuid, connection_id, table_name, cutoff_time
                )
                logger.info('Deleted %d records from connection %s table %s', 
                            total_deleted, connection_id, table_name)
            except Exception as e:
                logger.exception('Failed to clean deleted records for connection %s: %s', connection_id, e)

    def clear(self):
        self.should_stop.set()
        logger.info("Exit deleted records cleaner process")
        os._exit(0)

    def signal_term_handler(self, signal, frame):
        self.clear()

    def set_signal(self):
        signal.signal(signal.SIGTERM, self.signal_term_handler)


def start():
    deleted_records_cleaner = DeletedRecordsCleaner()
    logger.info("Deleted records cleaner process initialized.")
    try:
        deleted_records_cleaner.start()
    except Exception as e:
        logger.exception(e)
        deleted_records_cleaner.clear()
        os._exit(0)

    while True:
        time.sleep(2)


def main():
    LogConfigurator(config.LOG_LEVEL, config.LOG_FILE)
    start()


if __name__ == "__main__":
    main()
