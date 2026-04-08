import signal
import os

from seaqa_indexer.utils import run


def kill_web_crawler_task_worker():
    cmd = [
        'pkill', '-f', 'seaqa_indexer.crawler.web_crawl_task_worker'
    ]
    run(cmd)

def kill_connection_sync_task_worker():
    cmd = [
        'pkill', '-f', 'seaqa_indexer.syncer.connection_sync_worker'
    ]
    run(cmd)

def kill_index_task_worker():
    cmd = [
        'pkill', '-f', 'seaqa_indexer.index.index_task_worker'
    ]
    run(cmd)


def kill_ai_processor():
    cmd = [
        'pkill', '-f', 'seaqa_indexer.ai_processor.ai_process_task_worker'
    ]
    run(cmd)


def kill_summary_index_task_worker():
    cmd = [
        'pkill', '-f', 'seaqa_indexer.index.summary_index_task_worker'
    ]
    run(cmd)


def kill_content_vector_index_task_worker():
    cmd = [
        'pkill', '-f', 'seaqa_indexer.index.content_vector_index_task_worker'
    ]
    run(cmd)

def kill_deleted_records_cleaner():
    cmd = [
        'pkill', '-f', 'seaqa_indexer.tasks.deleted_records_cleaner'
    ]
    run(cmd)

def kill_notion_page_cleaner():
    cmd = [
        'pkill', '-f', 'seaqa_indexer.tasks.notion_page_cleaner'
    ]
    run(cmd)


def signal_term_handler(signal, frame):
    kill_web_crawler_task_worker()
    kill_index_task_worker()
    kill_connection_sync_task_worker()
    kill_ai_processor()
    kill_summary_index_task_worker()
    kill_content_vector_index_task_worker()
    kill_deleted_records_cleaner()
    os._exit(0)


def set_signal():
    # TODO: look like python will add signal to queue when cpu exec c extension code,
    # and will call signal callback method after cpu exec python code
    # ref: https://docs.python.org/2/library/signal.html
    signal.signal(signal.SIGTERM, signal_term_handler)
