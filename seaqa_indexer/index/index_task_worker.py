import logging

from seaqa_indexer.workers.base_task_worker import LockingTaskWorker, run_worker
from seaqa_indexer.index.index_utils import (
    update_connection_index,
    update_knowledge_base_index,
    update_ticket_index
)
from seaqa_indexer.db import init_db_session_class


logger = logging.getLogger(__name__)


class IndexTaskWorker(LockingTaskWorker):
    """The handler for redis message queue - index tasks"""

    def get_queue_name(self) -> str:
        return 'index_task'

    def get_worker_name(self) -> str:
        return 'index_task_worker'

    def get_lock_key(self, task_id) -> str:
        return f'index_{task_id}'

    def init_dependencies(self):
        self.db_session_class = init_db_session_class()

    def process_message(self, data: dict):
        task_type = data.get('type')
        if task_type == 'knowledge_base':
            project_uuid = data.get('project_uuid')
            self.kb_task_handler(project_uuid)
        elif task_type == 'ticket':
            project_uuid = data.get('project_uuid')
            self.ticket_task_handler(project_uuid)
        elif task_type == 'connection':
            connection_id = data.get('connection_id')
            self.connection_task_handler(connection_id)

    def connection_task_handler(self, connection_id):
        self.execute_with_lock(
            connection_id,
            self.update_index,
            connection_id
        )

    def kb_task_handler(self, project_uuid):
        self.execute_with_lock(
            f'kb_{project_uuid}',
            update_knowledge_base_index,
            project_uuid,
            self.db_session_class
        )

    def ticket_task_handler(self, project_uuid):
        self.execute_with_lock(
            f'ticket_{project_uuid}',
            update_ticket_index,
            project_uuid,
            self.db_session_class
        )

    def update_index(self, connection_id):
        try:
            update_connection_index(connection_id, self.db_session_class)
        except Exception as e:
            logger.exception('update connection: %s index failed, error: %s', connection_id, e)


def main():
    run_worker(IndexTaskWorker)


if __name__ == "__main__":
    main()
