# coding: UTF-8
import logging
import json

from sqlalchemy.sql import text
from threading import Thread, Event

from seaqa_indexer.config import LOG_FILE, LOG_LEVEL
from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.utils.constants import ConnectionType
from seaqa_indexer.mq import get_mq
from seaqa_indexer.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD


logger = logging.getLogger(__name__)

__all__ = [
    'SyncTaskPublisher',
]

SYNC_CONNECTION_TYPES = [
    ConnectionType.SEAFILE.value,
    ConnectionType.DISCOURSE_FORUM.value,
    ConnectionType.GITHUB_ISSUE.value,
    ConnectionType.GITHUB_PR.value,
    ConnectionType.EMAIL.value,
    ConnectionType.NOTION.value,
]


class SyncTaskPublisher(object):
    def __init__(self, sync_manager):
        self._enabled = True
        self._sync_manager = sync_manager
        self._logfile = LOG_FILE
        self._loglevel = LOG_LEVEL
        self._interval = 1 * 60 * 60
        self._sync_interval = 1 * 60 * 60

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start sync task publisher: it is not enabled!')
            return

        SyncTaskPublisherTimer(self._logfile, self._loglevel, self._interval, self._sync_interval, self._sync_manager).start()
        logging.info('sync task publisher is started')

    def is_enabled(self):
        return self._enabled


class SyncTaskPublisherTimer(Thread):
    def __init__(self, logfile, loglevel, interval, sync_interval, sync_manager):
        Thread.__init__(self)
        self._logfile = logfile
        self._loglevel = loglevel
        self.finished = Event()
        self._interval = interval
        self._sync_interval = sync_interval
        self.db_session_class = init_db_session_class()
        self.mq = get_mq(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)
        self.sync_manager = sync_manager

    def get_sync_project_connections_by_page(self, start, limit):
        with self.db_session_class() as session:
            sql = """
            SELECT pc.id, pc.config, pc.project_uuid, pc.status, p.uuid, pc.type, pc.last_sync_time
            FROM project_connection pc
            INNER JOIN projects p ON p.uuid = pc.project_uuid
            WHERE p.deleted = false AND pc.type in :connection_types
              AND (
                    pc.last_sync_time IS NULL
                    OR TIMESTAMPDIFF(SECOND, pc.last_sync_time, UTC_TIMESTAMP()) > :sync_interval
                  )
              AND JSON_UNQUOTE(JSON_EXTRACT(`status`, '$.last_sync_status')) != 'crawling'
            LIMIT :start, :limit
            """
            project_connections = session.execute(
                text(sql),
                {'start': start, 'limit': limit, 'connection_types': SYNC_CONNECTION_TYPES, 'sync_interval': self._sync_interval}
            ).fetchall()
            return project_connections

    def publish_sync_tasks(self):
        start, limit = 0, 1000
        while True:
            try:
                project_connections = self.get_sync_project_connections_by_page(start, limit)
            except Exception as e:
                logger.error("get syn project connection error: %s" % e)
                return
            start += 1000
            if len(project_connections) == 0:
                break
            for project_connection in project_connections:
                connection_id = project_connection.id
                try:
                    self.sync_manager.check_and_sync(connection_id)
                except Exception as e:
                    logger.error('cron publish sync task failed, connection_id: %s, error: %s', connection_id, e)

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self._interval)
            if not self.finished.is_set():
                logging.info('starts to publish sync task')
                try:
                    self.publish_sync_tasks()
                except Exception as e:
                    logging.exception('publish sync task error: %s', e)

    def cancel(self):
        self.finished.set()
