import sys
import time
import json
import logging
import threading
import signal
import datetime

from seaqa_indexer import config
from seaqa_indexer.app.log import LogConfigurator
from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.utils import decrypt_config, add_suffix_to_logfile
from seaqa_indexer.utils.seahub_api import SeahubAPI
from seaqa_indexer.seadb_models.seafile_seadb_api import SeafileSeaDBAPI
from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.utils.constants import ConnectionType, SYNC_FILE_SUFFIXES
from seaqa_indexer.utils import get_file_suffix

logger = logging.getLogger(__name__)

SEAFILE_FILE_CLEANER_LOG_SUFFIX = 'seafile_file_cleaner'

class SeafileFileCleaner(object):
    """Compare remote repo files with local records and mark missing files as deleted."""

    def __init__(self):
        self.db_session_class = init_db_session_class()
        self.seadb_api = SeaDBAPI()
        self.seaqa_db = SeaqaDB(self.db_session_class)

        self.should_stop = threading.Event()
        self.clear_interval = 24 * 60 * 60
        self.recent_days = 30 # Only check connections that are more than 30 days old since the last cleanup.
        self.set_signal()

    @property
    def tname(self):
        return threading.current_thread().name

    def start(self):
        threading.Thread(
            target=self.cron_clean_deleted_seafile_files,
            name='cron_clean_deleted_seafile_files_thread',
            daemon=True
        ).start()

    def cron_clean_deleted_seafile_files(self):
        logger.info('Start seafile deleted files cleaner timer')
        while not self.should_stop.is_set():
            self.should_stop.wait(self.clear_interval)

            if not self.should_stop.is_set():
                logger.info('Start to clean seafile files')
                try:
                    self.clean_deleted_files()
                except Exception as e:
                    logger.exception('Cron clean seafile files error: %s', e)

    def fetch_remote_file_ids(self, seahub_api):
        file_ids = set()
        page = 1
        per_page = 1000
        since = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc).isoformat()
        while True:
            resp = seahub_api.get_recently_changed_files(since, page, per_page)
            page += 1
            recently_changed_files = resp.get('files', [])
            if not recently_changed_files:
                break

            for file in recently_changed_files:
                dirent_name = file.get('_name')
                file_id = file.get('_id')

                file_suffix = get_file_suffix(dirent_name)
                if file_suffix not in SYNC_FILE_SUFFIXES:
                    continue
                file_ids.add(file_id)
            time.sleep(0.34)

        return file_ids

    def get_local_file_ids(self, seafile_seadb_api, connection_id):
        offset = 0
        since = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc).isoformat()
        page_size = 500
        file_id_to_pk = {}

        while True:
            rows = seafile_seadb_api.get_files_by_page(connection_id, since, page_size, offset)
            if not rows:
                break
            for row in rows:
                file_id = row.get('file_id')
                pk = row.get('_pk')
                file_id_to_pk[file_id] = pk
            if len(rows) < page_size:
                break
            offset += page_size
        return file_id_to_pk

    def clean_deleted_files(self):
        connections = self.seaqa_db.get_need_cleaned_connections_by_type(self.recent_days, ConnectionType.SEAFILE.value)
        logger.info('Found %d seafile connections to clean', len(connections))

        for connection in connections:
            connection_id = connection.id
            project_uuid = connection.project_uuid

            try:
                connection_config = decrypt_config(json.loads(connection.config))
                server_url = connection_config.get('server_url')
                api_token = connection_config.get('api_token')

                seahub_api = SeahubAPI(server_url, api_token)
                seafile_seadb_api = SeafileSeaDBAPI(project_uuid)

                remote_file_ids = self.fetch_remote_file_ids(seahub_api)
                
                file_id_to_pk = self.get_local_file_ids(seafile_seadb_api, connection_id)

                need_deleted_file_ids = set(file_id_to_pk.keys()) - set(remote_file_ids)

                seafile_seadb_api.mark_file_as_deleted(connection_id, need_deleted_file_ids, file_id_to_pk)

                cleaned_at = datetime.datetime.now(datetime.UTC)
                self.seaqa_db.update_connection_last_cleaned_at(connection_id, cleaned_at)
                logger.info('Marked %d local files deleted for connection %s', len(need_deleted_file_ids), connection_id)
            except Exception as e:
                logger.exception('Failed to clean seafile files for connection %s: %s', connection_id, e)

    def clear(self):
        self.should_stop.set()
        logger.info("Exit seafile file cleaner process")
        sys.exit(0)

    def signal_term_handler(self, signal, frame):
        self.clear()

    def set_signal(self):
        signal.signal(signal.SIGTERM, self.signal_term_handler)


def start():
    seafile_file_cleaner = SeafileFileCleaner()
    logger.info("seafile deleted files cleaner process initialized.")
    try:
        seafile_file_cleaner.start()
    except Exception as e:
        logger.exception(e)
        seafile_file_cleaner.clear()
        sys.exit(1)

    while True:
        time.sleep(2)


def main():
    logfile = add_suffix_to_logfile(config.LOG_FILE, SEAFILE_FILE_CLEANER_LOG_SUFFIX)
    LogConfigurator(config.LOG_LEVEL, logfile)
    start()


if __name__ == "__main__":
    main()
