import os
import time
import logging
import threading
import signal

from sqlalchemy.sql import text

from seaqa_indexer import config
from seaqa_indexer.app.log import LogConfigurator
from seaqa_indexer.seasearch.index_manager import IndexManager
from seaqa_indexer.mq import get_mq
from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.crawler.storage import get_storage
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.utils.constants import ConnectionType

from seaqa_indexer.seadb_models.discourse_seadb_api import DiscourseSeaDBAPI

logger = logging.getLogger(__name__)


class ProjectConnectionCleaner(object):
    def __init__(self):
        self.db_session_class = init_db_session_class()
        self.index_manager = IndexManager()
        self.should_stop = threading.Event()
        self.clear_interval = 24 * 60 * 60

        self.LOCK_TIMEOUT = 1800  # 30 minutes
        self.locked_keys = set()

        self.mq = get_mq(config.REDIS_HOST, config.REDIS_PORT, config.REDIS_PASSWORD)
        self.storage = get_storage()
        self.seadb_api = SeaDBAPI()

        self.set_signal()

    @property
    def tname(self):
        return threading.current_thread().name

    def _get_index_lock_key(self, connection_id):
        return 'index_' + str(connection_id)

    def start(self):
        if not self.mq:
            return
        threading.Thread(target=self.cron_clear_project_connections, name='cron_clear_project_connections_thread', daemon=True).start()

    def cron_clear_project_connections(self):
        logger.info('Start to clear project connections timer')
        while not self.should_stop.is_set():
            self.should_stop.wait(self.clear_interval)

            if not self.should_stop.is_set():
                logger.info('start to clear project connections')
                try:
                    self.clear_project_connections()
                except Exception as e:
                    logger.exception('cron clear project connections error: %s', e)

    def get_all_deleted_connections(self):
        """get connection deleted"""
        with self.db_session_class() as session:
            sql = """SELECT pc.id, pc.config, pc.status, p.uuid as project_uuid, pc.type, pc.last_sync_time, pc.indexed_at
             FROM project_connection pc
            INNER JOIN projects p
            ON p.uuid=pc.project_uuid WHERE pc.deleted=true """
            connections = session.execute(text(sql)).fetchall()
            return connections

    def get_all_deleted_projects(self):
        with self.db_session_class() as session:
            sql = """SELECT * FROM deleted_projects"""
            connections = session.execute(text(sql)).fetchall()
            return connections

    def delete_deleted_project(self, project_uuid):
        with self.db_session_class() as session:
            sql = """DELETE FROM project_connection WHERE project_uuid=:project_uuid"""
            session.execute(text(sql), {'project_uuid': project_uuid})
            session.commit()
        with self.db_session_class() as session:
            sql = """DELETE FROM deleted_projects WHERE project_uuid=:project_uuid"""
            session.execute(text(sql), {'project_uuid': project_uuid})
            session.commit()

    def delete_deleted_connection(self, connection_id):
        with self.db_session_class() as session:
            sql = """DELETE FROM project_connection WHERE id=:connection_id"""
            session.execute(text(sql), {'connection_id': connection_id})
            session.commit()

    def delete_issues_from_db(self, connection_id):
        with self.db_session_class() as session:
            sql = text("DELETE FROM github_issues WHERE connection_id = :connection_id")
            session.execute(sql, {'connection_id': connection_id})
            session.commit()

    def delete_discourse_data_from_db(self, project_uuid, connection_id):
        """Delete discourse topics and replies from seadb"""
        try:
            discourse_seadb_api = DiscourseSeaDBAPI(project_uuid)

            deleted_count = discourse_seadb_api.delete_all_data_by_connection_id(connection_id)
            logger.info(f"Deleted {deleted_count} discourse records for connection {connection_id} from seadb")
        except Exception as e:
            logger.error(f"Error deleting discourse data from seadb for connection {connection_id}: {e}")
            raise

    def clear_deleted_project(self, project_uuid):
        # delete project index
        self.index_manager.delete_index_by_project(project_uuid)

        # delete crawled file folder
        self.storage.delete_project(project_uuid)

        self.seadb_api.delete_base(project_uuid)
        # delete db record
        self.delete_deleted_project(project_uuid)
        logger.info('Clear project %s success', project_uuid)

    def clear_deleted_connection(self, project_uuid, connection_id, connection_type):
        # delete connection index
        self.index_manager.delete_index_by_connection(connection_id, connection_type)

        if connection_type == ConnectionType.SITE.value:
            self.storage.delete_site(connection_id, project_uuid)

            base_metadata = self.seadb_api.get_base_metadata(project_uuid)
            tables = base_metadata.get('tables')
            if tables:
                for table in tables:
                    if table['name'] == str(connection_id):
                        self.seadb_api.delete_table(project_uuid, table['id'])
        elif connection_type == ConnectionType.GITHUB_ISSUE.value:
            self.delete_issues_from_db(connection_id)
        elif connection_type == ConnectionType.DISCOURSE_FORUM.value:
            self.delete_discourse_data_from_db(project_uuid, connection_id)

        # delete db record
        self.delete_deleted_connection(connection_id)
        logger.info('Clear connection %s success', connection_id)

    def clear_project_connections(self):
        # delete connection
        deleted_projects = self.get_all_deleted_projects()
        for project in deleted_projects:
            project_uuid = project.project_uuid
            logger.info('Start clean project %s', project_uuid)
            try:
                self.clear_deleted_project(project_uuid)
            except Exception as e:
                logger.exception('Fail to clear project %s, error: %s', project_uuid, e)
            logger.info('Finish clean project %s success', project_uuid)

        # delete connections
        deleted_connections = self.get_all_deleted_connections()
        for connection in deleted_connections:
            project_uuid = connection.project_uuid
            connection_id = connection.id
            connection_type = connection.type

            if self.mq.set(self._get_index_lock_key(connection_id), time.time(), ex=self.LOCK_TIMEOUT, nx=True):
                # get lock
                logger.info('%s Start deleting connection %s' % (self.tname, connection_id))
                lock_key = self._get_index_lock_key(connection_id)
                self.locked_keys.add(lock_key)
                try:
                    self.clear_deleted_connection(project_uuid, connection_id, connection_type)
                except Exception as e:
                    logger.exception('Fail to clear deleted connection, project_uuid: %s, connection_id: %s, error: %s', project_uuid, connection_id, e)
                try:
                    self.locked_keys.remove(lock_key)
                except KeyError:
                    logger.error("%s is already removed. SHOULD NOT HAPPEN!" % lock_key)
                self.mq.delete(lock_key)
                logger.info('Thread %s Delete connection %s completed' % (self.tname, connection_id))
            else:
                logger.warning('Delete connection %s pass, because of update index is running.', connection_id)

    def clear(self):
        if not self.mq:
            return
        self.should_stop.set()
        # if a thread just lock key, wait to add the lock to the list.
        time.sleep(1)
        # del redis locked key
        for key in self.locked_keys:
            self.mq.delete(key)
            logger.info("redis lock key %s has been deleted", key)
        # sys.exit
        logger.info("Exit project connection cleaner process")
        os._exit(0)

    def signal_term_handler(self, signal, frame):
        self.clear()

    def set_signal(self):
        # TODO: look like python will add signal to queue when cpu exec c extension code,
        # and will call signal callback method after cpu exec python code
        # ref: https://docs.python.org/2/library/signal.html
        signal.signal(signal.SIGTERM, self.signal_term_handler)


def start():
    project_connection_cleaner = ProjectConnectionCleaner()
    logger.info("Project connection cleaner process initialized.")
    try:
        project_connection_cleaner.start()
    except Exception as e:
        logger.exception(e)
        project_connection_cleaner.clear()
        os._exit(0)

    while True:
        # if main thread has been quit or join for subthread.
        # signal callback will never be  call.
        time.sleep(2)


def main():
    LogConfigurator(config.LOG_LEVEL, config.LOG_FILE)
    start()


if __name__ == "__main__":
    main()
