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
    'WebCrawlTaskPublisher',
]


class WebCrawlTaskPublisher(object):
    def __init__(self):
        self._enabled = True
        self._logfile = LOG_FILE
        self._loglevel = LOG_LEVEL
        self._interval = 1 * 60 * 60
        self._crawl_interval = 3 * 24 * 60 * 60

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start web crawl task publisher: it is not enabled!')
            return

        WebCrawlTaskPublisherTimer(self._logfile, self._loglevel, self._interval, self._crawl_interval).start()
        logging.info('web crawl task publisher is started')

    def is_enabled(self):
        return self._enabled


class WebCrawlTaskPublisherTimer(Thread):
    def __init__(self, logfile, loglevel, interval, crawl_interval):
        Thread.__init__(self)
        self._logfile = logfile
        self._loglevel = loglevel
        self.finished = Event()
        self._interval = interval
        self._crawl_interval = crawl_interval
        self.db_session_class = init_db_session_class()
        self.mq = get_mq(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)

    def get_web_crawl_project_connections_by_page(self, start, limit):
        with self.db_session_class() as session:
            sql = """
            SELECT pc.id
            FROM project_connection pc
            INNER JOIN projects p ON p.uuid = pc.project_uuid
            WHERE p.deleted = false AND pc.type=:connection_type
              AND (
                    pc.last_sync_time IS NULL
                    OR TIMESTAMPDIFF(SECOND, pc.last_sync_time, UTC_TIMESTAMP()) > :crawl_interval
                  )
            LIMIT :start, :limit
            """
            project_connections = session.execute(
                text(sql),
                {'start': start, 'limit': limit, 'connection_type': 'site',  'crawl_interval': self._crawl_interval}
            ).fetchall()
            return project_connections

    def publish_web_crawl_tasks(self):
        start, limit = 0, 1000
        while True:
            try:
                project_connections = self.get_web_crawl_project_connections_by_page(start, limit)
            except Exception as e:
                logger.error("get web crawl project connection error: %s" % e)
                return
            start += 1000
            if len(project_connections) == 0:
                break
            for project_connection in project_connections:
                connection_id = project_connection.id
                try:
                    self.add_crawl_site_task(connection_id)
                except Exception as e:
                    logger.error('cron publish web crawl task failed, connection_id: %s, error: %s', connection_id, e)

    def add_crawl_site_task(self, connection_id):
        msg_content = {
            'site_id': connection_id,
        }
        self.mq.lpush('web_crawl_task', json.dumps(msg_content))

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self._interval)
            if not self.finished.is_set():
                logging.info('starts to publish web crawl task')
                try:
                    self.publish_web_crawl_tasks()
                except Exception as e:
                    logging.exception('publish web crawl task error: %s', e)

    def cancel(self):
        self.finished.set()
