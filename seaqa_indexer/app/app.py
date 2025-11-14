from seaqa_indexer.server.http_server import HttpServer
from seaqa_indexer.syncer.github_issues_syncer.github_issues_sync_manager import GithubIssuesSyncManager
from seaqa_indexer.workers.web_crawl_worker_starter import WebCrawlWorkerStarter
from seaqa_indexer.crawler.web_crawl_manager import WebCrawlManager
from seaqa_indexer.seasearch.index_manager import IndexManager
from seaqa_indexer.workers.index_worker_starter import IndexWorkerStarter
from seaqa_indexer.tasks.project_connection_cleaner_starter import ProjectConnectionCleanerStarter
from seaqa_indexer.workers.connection_sync_starter import ConnectionSyncWorkerStarter
from seaqa_indexer.syncer.discourse_syncer.discourse_sync_manager import DiscourseSyncManager
from seaqa_indexer.syncer.seafile_syncer.seafile_sync_manager import SeafileSyncManager
from seaqa_indexer.tasks.sync_task_publisher import SyncTaskPublisher
from seaqa_indexer.tasks.web_crawl_task_publisher import WebCrawlTaskPublisher
from seaqa_indexer.workers.ai_process_worker_starter import AIProcessWorkerStarter

class SeaqaIndexerApp(object):
    def __init__(self, ):
        self.web_crawl_manager = WebCrawlManager()
        self.index_manager = IndexManager()
        self.github_issues_sync_manager = GithubIssuesSyncManager()
        self.discourse_sync_manager = DiscourseSyncManager()
        self.seafile_sync_manager = SeafileSyncManager()

        self.web_crawl_worker_starter = WebCrawlWorkerStarter()
        self.index_worker_starter = IndexWorkerStarter()
        self.connection_sync_starter = ConnectionSyncWorkerStarter()
        self.ai_process_worker_starter = AIProcessWorkerStarter()

        self.http_server = HttpServer(self)
        self.project_connection_cleaner_starter = ProjectConnectionCleanerStarter()
        self.sync_task_publisher = SyncTaskPublisher()
        self.web_crawl_task_publisher = WebCrawlTaskPublisher()

    def serve_forever(self):
        self.web_crawl_worker_starter.start()
        self.index_worker_starter.start()
        self.connection_sync_starter.start()
        self.ai_process_worker_starter.start()

        self.project_connection_cleaner_starter.start()
        self.sync_task_publisher.start()
        self.web_crawl_task_publisher.start()
        self.http_server.start()
