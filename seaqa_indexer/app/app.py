from seaqa_indexer.server.http_server import HttpServer
from seaqa_indexer.workers.web_crawl_worker_starter import WebCrawlWorkerStarter
from seaqa_indexer.crawler.web_crawl_manager import WebCrawlManager
from seaqa_indexer.seasearch.index_manager import IndexManager
from seaqa_indexer.workers.index_worker_starter import IndexWorkerStarter
from seaqa_indexer.workers.ai_index_worker_starter import AIIndexWorkerStarter
from seaqa_indexer.tasks.project_connection_cleaner_starter import ProjectConnectionCleanerStarter
from seaqa_indexer.workers.connection_sync_starter import ConnectionSyncWorkerStarter
from seaqa_indexer.tasks.sync_task_publisher import SyncTaskPublisher
from seaqa_indexer.tasks.web_crawl_task_publisher import WebCrawlTaskPublisher
from seaqa_indexer.workers.ai_process_worker_starter import AIProcessWorkerStarter
from seaqa_indexer.workers.content_vector_index_worker_starter import ContentVectorIndexWorkerStarter
from seaqa_indexer.tasks.deleted_records_cleaner_starter import DeletedRecordsCleanerStarter
from seaqa_indexer.seasearch.knowledge_base_index_master import KnowledgeBaseIndexMaster
from seaqa_indexer.seasearch.ticket_index_master import TicketIndexMaster
from seaqa_indexer.syncer.sync_manager import SyncManager


class SeaqaIndexerApp(object):
    def __init__(self, ):
        self.web_crawl_manager = WebCrawlManager()
        self.index_manager = IndexManager()
        self.sync_manager = SyncManager()

        self.web_crawl_worker_starter = WebCrawlWorkerStarter()
        self.index_worker_starter = IndexWorkerStarter()
        self.ai_index_worker_starter = AIIndexWorkerStarter()
        self.connection_sync_starter = ConnectionSyncWorkerStarter()
        self.ai_process_worker_starter = AIProcessWorkerStarter()
        self.content_vector_index_worker_starter = ContentVectorIndexWorkerStarter()
        self.kb_index_master = KnowledgeBaseIndexMaster()
        self.ticket_index_master = TicketIndexMaster()

        self.http_server = HttpServer(self)
        self.project_connection_cleaner_starter = ProjectConnectionCleanerStarter()
        self.sync_task_publisher = SyncTaskPublisher()
        self.web_crawl_task_publisher = WebCrawlTaskPublisher()
        self.deleted_records_cleaner_starter = DeletedRecordsCleanerStarter()

    def serve_forever(self):
        self.web_crawl_worker_starter.start()
        self.index_worker_starter.start()
        self.ai_index_worker_starter.start()
        self.connection_sync_starter.start()
        self.ai_process_worker_starter.start()
        self.content_vector_index_worker_starter.start()
        self.kb_index_master.start()
        self.ticket_index_master.start()

        self.deleted_records_cleaner_starter.start()
        self.project_connection_cleaner_starter.start()
        self.sync_task_publisher.start()
        self.web_crawl_task_publisher.start()
        self.http_server.start()
