import logging

from seaqa_indexer.server.http_server import HttpServer
from seaqa_indexer.crawler.web_crawl_manager import WebCrawlManager
from seaqa_indexer.seasearch.index_manager import IndexManager
from seaqa_indexer.tasks.project_connection_cleaner_starter import ProjectConnectionCleanerStarter
from seaqa_indexer.tasks.sync_task_publisher import SyncTaskPublisher
from seaqa_indexer.tasks.web_crawl_task_publisher import WebCrawlTaskPublisher
from seaqa_indexer.tasks.deleted_records_cleaner_starter import DeletedRecordsCleanerStarter
from seaqa_indexer.seasearch.knowledge_base_index_master import KnowledgeBaseIndexMaster
from seaqa_indexer.seasearch.ticket_index_master import TicketIndexMaster
from seaqa_indexer.syncer.sync_manager import SyncManager
from seaqa_indexer.utils import get_python_executable, run


class SeaqaIndexerApp(object):
    def __init__(self, ):
        self.web_crawl_manager = WebCrawlManager()
        self.sync_manager = SyncManager()

        self.index_manager = IndexManager()
        self.kb_index_master = KnowledgeBaseIndexMaster()
        self.ticket_index_master = TicketIndexMaster()

        self.http_server = HttpServer(self)
        self.sync_task_publisher = SyncTaskPublisher()
        self.web_crawl_task_publisher = WebCrawlTaskPublisher()
        self.project_connection_cleaner_starter = ProjectConnectionCleanerStarter()
        self.deleted_records_cleaner_starter = DeletedRecordsCleanerStarter()

    def serve_forever(self):
        self._start_worker('seaqa_indexer.crawler.web_crawl_task_worker', 'web crawl worker')
        self._start_worker('seaqa_indexer.seasearch.index_task_worker', 'index worker')
        self._start_worker('seaqa_indexer.seasearch.summary_index_task_worker', 'ai index worker')
        self._start_worker('seaqa_indexer.syncer.connection_sync_worker', 'connection sync worker')
        self._start_worker('seaqa_indexer.ai_processor.ai_process_task_worker', 'ai process worker')
        self._start_worker('seaqa_indexer.seasearch.content_vector_index_task_worker', 'content vector index worker')

        self.sync_manager.start()
        
        self.kb_index_master.start()
        self.ticket_index_master.start()

        self.deleted_records_cleaner_starter.start()
        self.project_connection_cleaner_starter.start()
        self.sync_task_publisher.start()
        self.web_crawl_task_publisher.start()
        self.http_server.start()

    def _start_worker(self, module_name, worker_label):
        try:
            cmd = [get_python_executable(), '-m', module_name]
            run(cmd)
            logging.info('%s is started', worker_label)
        except Exception as e:
            logging.exception('error when start %s: %s', worker_label, e)
