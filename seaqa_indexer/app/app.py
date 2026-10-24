import logging
import threading
import time

from seaqa_indexer.config import WORKER_CHECK_INTERVAL
from seaqa_indexer.server.http_server import HttpServer
from seaqa_indexer.crawler.web_crawl_manager import WebCrawlManager
from seaqa_indexer.index.index_manager import IndexManager
from seaqa_indexer.tasks.linear_issues_cleaner import LinearIssuesCleaner
from seaqa_indexer.tasks.sync_task_publisher import SyncTaskPublisher
from seaqa_indexer.tasks.notion_page_cleaner import NotionPageCleaner
from seaqa_indexer.tasks.seafile_file_cleaner import SeafileFileCleaner
from seaqa_indexer.tasks.confluence_page_cleaner import ConfluencePageCleaner
from seaqa_indexer.tasks.discord_message_cleaner import DiscordMessageCleaner
from seaqa_indexer.tasks.confluence_privacy_reporter import ConfluencePrivacyReporter
from seaqa_indexer.tasks.jira_privacy_reporter import JiraPrivacyReporter
from seaqa_indexer.tasks.deleted_records_cleaner import DeletedRecordsCleaner
from seaqa_indexer.tasks.github_options_syncer import GithubOptionsSyncer
from seaqa_indexer.tasks.web_crawl_task_publisher import WebCrawlTaskPublisher
from seaqa_indexer.index.knowledge_base_index_master import KnowledgeBaseIndexMaster
from seaqa_indexer.index.ticket_index_master import TicketIndexMaster
from seaqa_indexer.index.portal_issue_index_master import PortalIssueIndexMaster
from seaqa_indexer.syncer.sync_manager import SyncManager
from seaqa_indexer.utils import get_python_executable, run
from seaqa_indexer.utils.attachment_backend import get_backend
from seaqa_indexer.app.signal_handler import _shutdown


class SeaqaIndexerApp(object):
    def __init__(self, ):
        self._worker_processes = {}

        self.web_crawl_manager = WebCrawlManager()
        self.sync_manager = SyncManager()

        self.index_manager = IndexManager()
        self.kb_index_master = KnowledgeBaseIndexMaster()
        self.ticket_index_master = TicketIndexMaster()
        self.portal_issue_index_master = PortalIssueIndexMaster()

        self.http_server = HttpServer(self)
        self.sync_task_publisher = SyncTaskPublisher(self.sync_manager)
        self.notion_page_cleaner = NotionPageCleaner()
        self.seafile_file_cleaner = SeafileFileCleaner()
        self.deleted_records_cleaner = DeletedRecordsCleaner()
        self.linear_issues_cleaner = LinearIssuesCleaner()
        self.confluence_page_cleaner = ConfluencePageCleaner()
        self.github_options_syncer = GithubOptionsSyncer()
        self.discord_message_cleaner = DiscordMessageCleaner()
        self.confluence_privacy_reporter = ConfluencePrivacyReporter()
        self.jira_privacy_reporter = JiraPrivacyReporter()

        self.web_crawl_task_publisher = WebCrawlTaskPublisher()

    def serve_forever(self):
        # Initialize boto3 before starting service threads that may access storage.
        get_backend()
        self._start_worker('seaqa_indexer.crawler.web_crawl_task_worker', 'web crawl worker')
        self._start_worker('seaqa_indexer.index.index_task_worker', 'index worker')
        self._start_worker('seaqa_indexer.index.summary_index_task_worker', 'ai index worker')
        self._start_worker('seaqa_indexer.syncer.connection_sync_worker', 'connection sync worker')
        self._start_worker('seaqa_indexer.ai_processor.ai_process_task_worker', 'ai process worker')
        self._start_worker('seaqa_indexer.index.content_vector_index_task_worker', 'content vector index worker')

        self._start_worker_monitor()

        self.sync_manager.start()

        self.kb_index_master.start()
        self.ticket_index_master.start()
        self.portal_issue_index_master.start()

        self.sync_task_publisher.start()
        self.web_crawl_task_publisher.start()
        self.notion_page_cleaner.start()
        self.seafile_file_cleaner.start()
        self.deleted_records_cleaner.start()
        self.linear_issues_cleaner.start()
        self.confluence_page_cleaner.start()
        self.github_options_syncer.start()
        self.discord_message_cleaner.start()
        self.confluence_privacy_reporter.start()
        self.jira_privacy_reporter.start()
        self.http_server.start()
        self.http_server.join()

    def _start_worker_monitor(self):
        def monitor():
            while not _shutdown:
                time.sleep(WORKER_CHECK_INTERVAL)
                for worker_label in list(self._worker_processes.keys()):
                    info = self._worker_processes.get(worker_label)
                    if info is None:
                        continue
                    proc = info['process']
                    if proc.poll() is not None:
                        logging.warning('%s (PID: %d) has exited with code %s, restarting...',
                                        worker_label, proc.pid, proc.returncode)
                        self._start_worker(info['module_name'], worker_label)

        t = threading.Thread(target=monitor, daemon=True)
        t.start()

    def _start_worker(self, module_name, worker_label):
        try:
            cmd = [get_python_executable(), '-m', module_name]
            proc = run(cmd)
            self._worker_processes[worker_label] = {
                'process': proc,
                'module_name': module_name,
            }
            logging.info('%s (PID: %d) is started', worker_label, proc.pid)
        except Exception as e:
            logging.exception('error when start %s: %s', worker_label, e)
