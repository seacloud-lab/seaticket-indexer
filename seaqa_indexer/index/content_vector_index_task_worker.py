import logging

from seaqa_indexer.workers.base_task_worker import BaseTaskWorker, run_worker
from seaqa_indexer.index.index_utils import update_connection_content_vector_index
from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.utils.seadb_api import SeaDBAPI
from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.crawler.storage import get_storage
from seaqa_indexer.utils.seasearch_api import SeaSearchAPI
from seaqa_indexer.config import SEASEARCH_URL, SEASEARCH_TOKEN


logger = logging.getLogger(__name__)


class ContentVectorIndexTaskWorker(BaseTaskWorker):
    """Worker for processing content vector indexing tasks"""

    def get_queue_name(self) -> str:
        return 'content_vector_index_task'

    def get_worker_name(self) -> str:
        return 'content_vector_index'

    def init_dependencies(self):
        self.db_session_class = init_db_session_class()
        self.seadb_api = SeaDBAPI()
        self.seaqa_db = SeaqaDB(self.db_session_class)
        self.storage = get_storage()
        self.seasearch_api = SeaSearchAPI(SEASEARCH_URL, SEASEARCH_TOKEN)

    def process_message(self, data: dict):
        connection_id = data.get('connection_id')
        self.work_task_handler(connection_id)

    def work_task_handler(self, connection_id):
        try:
            update_connection_content_vector_index(connection_id, self.seaqa_db, self.seadb_api, self.storage, self.seasearch_api)
        except Exception as e:
            logger.exception('Content vector index connection: %s failed, error: %s', connection_id, e)


def main():
    run_worker(ContentVectorIndexTaskWorker)


if __name__ == "__main__":
    main()
