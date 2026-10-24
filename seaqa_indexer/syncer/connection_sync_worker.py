import logging

from seaqa_indexer.workers.base_task_worker import BaseTaskWorker, run_worker
from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.syncer.sync_connection import sync_connection
from seaqa_indexer.utils.attachment_backend import get_backend
from seaqa_indexer.utils.seaqa_db import SeaqaDB


logger = logging.getLogger(__name__)


class ConnectionSyncTaskWorker(BaseTaskWorker):
    """The handler for redis message queue - sync tasks"""

    def get_queue_name(self) -> str:
        return 'sync_task'

    def get_worker_name(self) -> str:
        return 'connection_sync_worker'

    def init_dependencies(self):
        self.db_session_class = init_db_session_class()
        self.seaqa_db = SeaqaDB(self.db_session_class)

    def process_message(self, data: dict):
        connection_id = data.get('connection_id')
        self.work_task_handler(connection_id)

    def work_task_handler(self, connection_id):
        logger.info('%s start sync connection: %s' % (self.tname, connection_id))
        try:
            sync_connection(connection_id, self.seaqa_db, self.mq, self.db_session_class)
        except Exception as e:
            logger.exception('%s sync connection %s error: %s' % (self.tname, connection_id, e))
        logger.info("%s Finish sync connection: %s" % (self.tname, connection_id))


def main():
    # Email attachment writes run in daemon task threads; initialize boto3 first.
    get_backend()
    run_worker(ConnectionSyncTaskWorker)


if __name__ == "__main__":
    main()
