import logging

from seaqa_indexer.workers.base_task_worker import LockingTaskWorker, run_worker
from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.utils.seasearch_api import SeaSearchAPI
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.index.index_utils import (
    update_tickets_summary_index,
    update_connection_summary_index,
    update_knowledge_base_summary_index
)
from seaqa_indexer.config import SEASEARCH_URL, SEASEARCH_TOKEN


logger = logging.getLogger(__name__)


class SummaryIndexTaskWorker(LockingTaskWorker):
    """Worker for processing AI summary vector indexing tasks"""

    @property
    def worker_num(self) -> int:
        return 2

    def get_queue_name(self) -> str:
        return 'summary_index_task'

    def get_worker_name(self) -> str:
        return 'summary_index_task_worker'

    def get_lock_key(self, task_id) -> str:
        return f'summary_index_{task_id}'

    def init_dependencies(self):
        self.db_session_class = init_db_session_class()
        self.seadb_api = SeaDBAPI()
        self.seaqa_db = SeaqaDB(self.db_session_class)
        self.seasearch_api = SeaSearchAPI(SEASEARCH_URL, SEASEARCH_TOKEN)

    def process_message(self, data: dict):
        task_type = data.get('type', 'connection')
        if task_type == 'knowledge_base':
            project_uuid = data.get('project_uuid')
            self.kb_task_handler(project_uuid)
        elif task_type == 'ticket':
            project_uuid = data.get('project_uuid')
            self.tickets_task_handler(project_uuid)
        elif task_type == 'connection':
            connection_id = data.get('connection_id')
            self.work_task_handler(connection_id)
        else:
            logger.warning('summary_index_task: invalid type: %s', task_type)

    def work_task_handler(self, connection_id):
        self.execute_with_lock(
            connection_id,
            self.update_ai_index,
            connection_id
        )

    def kb_task_handler(self, project_uuid):
        self.execute_with_lock(
            f'kb_{project_uuid}',
            self.update_kb_ai_index,
            project_uuid
        )

    def tickets_task_handler(self, project_uuid):
        self.execute_with_lock(
            f'ticket_{project_uuid}',
            self.update_tickets_ai_index,
            project_uuid
        )
    def update_tickets_ai_index(self, project_uuid):
        try:
            update_tickets_summary_index(project_uuid, self.seaqa_db, self.seadb_api, self.seasearch_api)
        except Exception as e:
            logger.exception('Summary index tickets: %s failed, error: %s', project_uuid, e)

    def update_ai_index(self, connection_id):
        try:
            update_connection_summary_index(connection_id, self.seaqa_db, self.seadb_api, self.seasearch_api)
        except Exception as e:
            logger.exception('Summary index connection: %s failed, error: %s', connection_id, e)

    def update_kb_ai_index(self, project_uuid):
        try:
            update_knowledge_base_summary_index(project_uuid, self.seaqa_db, self.seadb_api, self.seasearch_api)
        except Exception as e:
            logger.exception('Summary index knowledge base of project: %s failed, error: %s', project_uuid, e)


def main():
    run_worker(SummaryIndexTaskWorker)


if __name__ == "__main__":
    main()
