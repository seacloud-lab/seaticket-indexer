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
    'ProjectConnectionTaskPublisher',
]


class ProjectConnectionTaskPublisher(object):
    def __init__(self):
        self._enabled = True
        self._logfile = LOG_FILE
        self._loglevel = LOG_LEVEL
        self._interval = 24 * 60 * 60
        self._sync_interval = 3 * 24 * 60 * 60

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start project connection task publisher: it is not enabled!')
            return

        ProjectConnectionTaskPublisherTimer(self._logfile, self._loglevel, self._interval, self._sync_interval).start()
        logging.info('project connection task publisher is started')

    def is_enabled(self):
        return self._enabled


class ProjectConnectionTaskPublisherTimer(Thread):
    def __init__(self, logfile, loglevel, interval, sync_interval):
        Thread.__init__(self)
        self._logfile = logfile
        self._loglevel = loglevel
        self.finished = Event()
        self._interval = interval
        self._sync_interval = sync_interval
        self.db_session_class = init_db_session_class()
        self.mq = get_mq(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)

    def get_project_connections_by_page(self, start, limit):
        with self.db_session_class() as session:
            sql = """
            SELECT pc.id, pc.config, pc.project_id, pc.status, p.uuid, pc.type, pc.last_sync_time
            FROM project_connection pc
            INNER JOIN projects p ON p.id = pc.project_id
            WHERE p.deleted = false
              AND (
                    pc.last_sync_time IS NULL
                    OR TIMESTAMPDIFF(SECOND, pc.last_sync_time, UTC_TIMESTAMP()) > :sync_interval
                  )
            LIMIT :start, :limit
            """
            project_connections = session.execute(
                text(sql),
                {'start': start, 'limit': limit, 'sync_interval': self._sync_interval}
            ).fetchall()
            return project_connections

    def publish_project_connection_tasks(self):
        start, limit = 0, 1000
        while True:
            try:
                project_connections = self.get_project_connections_by_page(start, limit)
            except Exception as e:
                logger.error("Error: %s" % e)
                return
            start += 1000
            if len(project_connections) == 0:
                break
            for project_connection in project_connections:
                connection_id = project_connection.id
                connection_type = project_connection.type
                try:
                    if connection_type == ConnectionType.SITE.value:
                        self.add_crawl_site_task(connection_id)
                    elif connection_type == ConnectionType.SEAFILE.value:
                        self.add_seafile_index_task(connection_id)
                    elif connection_type == ConnectionType.GITHUB_ISSUE.value:
                        self.add_github_issues_sync_task(connection_id)
                    elif connection_type == ConnectionType.DISCOURSE_FORUM.value:
                        self.add_discourse_sync_task(connection_id)
                    else:
                        logger.error('connection_id: %s, type: %s, not supported.', connection_id, connection_type)
                except Exception as e:
                    logger.error('cron publish project connection task failed, connection_id: %s, error: %s', connection_id, e)

    def add_crawl_site_task(self, connection_id):
        msg_content = {
            'site_id': connection_id,
        }
        self.mq.lpush('web_crawl_task', json.dumps(msg_content))

    def add_seafile_index_task(self, connection_id):
        msg_content = {
            'connection_id': connection_id,
        }
        self.mq.lpush('index_task', json.dumps(msg_content))

    def add_discourse_sync_task(self, connection_id):
        msg_content = {
            'connection_id': connection_id,
        }
        self.mq.lpush('sync_task', json.dumps(msg_content))

    def add_github_issues_sync_task(self, connection_id):
        logger.info('add github issues sync task: %s', connection_id)
        msg_content = {
            'connection_id': connection_id,
        }
        self.mq.lpush('sync_task', json.dumps(msg_content))

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self._interval)
            if not self.finished.is_set():
                logging.info('starts to project connection task')
                try:
                    self.publish_project_connection_tasks()
                except Exception as e:
                    logging.exception('error when index files: %s', e)

    def cancel(self):
        self.finished.set()
