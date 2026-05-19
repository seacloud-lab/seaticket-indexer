import time
import json
import logging
import datetime
from threading import Thread, Event

from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.utils import decrypt_config
from seaqa_indexer.utils.seahub_api import SeahubAPI
from seaqa_indexer.seadb_models.seafile_seadb_api import SeafileSeaDBAPI
from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.utils.constants import ConnectionType, SYNC_FILE_SUFFIXES
from seaqa_indexer.utils import get_file_suffix

logger = logging.getLogger(__name__)


class SeafileFileCleaner(object):
    def __init__(self):
        self._enabled = True
        self._interval = 24 * 60 * 60 # Running interval
        self.check_days = 30 # Only check connections that are more than 30 days old since the last cleanup.

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start seafile file cleaner: it is not enabled!')
            return

        SeafileFileCleanerTimer(self._interval, self.check_days).start()
        logging.info('seafile file cleaner timer is started')

    def is_enabled(self):
        return self._enabled

class SeafileFileCleanerTimer(Thread):
    """Compare remote repo files with local records and mark missing files as deleted."""

    def __init__(self, interval, check_days):
        Thread.__init__(self)
        self.db_session_class = init_db_session_class()
        self.seadb_api = SeaDBAPI()
        self.seaqa_db = SeaqaDB(self.db_session_class)

        self.finished = Event()
        self.interval = interval
        self.check_days = check_days


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
        connections = self.seaqa_db.get_need_cleaned_connections_by_type(self.check_days, ConnectionType.SEAFILE.value)
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

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self.interval)
            if not self.finished.is_set():
                logging.info('starts to clean seafile files task')
                try:
                    self.clean_deleted_files()
                except Exception as e:
                    logging.exception('clean seafile files task error: %s', e)

    def cancel(self):
        self.finished.set()
