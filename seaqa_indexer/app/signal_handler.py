import os
import signal

from seaqa_indexer.utils import run


def kill_web_crawler_task_worker():
    cmd = [
        'pkill', '-f', 'seaqa_indexer.crawler.web_crawl_task_worker'
    ]
    run(cmd)


def kill_index_task_worker():
    cmd = [
        'pkill', '-f', 'seaqa_indexer.seasearch.index_task_worker'
    ]
    run(cmd)


def signal_term_handler(signal, frame):
    kill_web_crawler_task_worker()
    kill_index_task_worker()
    os._exit(0)


def set_signal():
    # TODO: look like python will add signal to queue when cpu exec c extension code,
    # and will call signal callback method after cpu exec python code
    # ref: https://docs.python.org/2/library/signal.html
    signal.signal(signal.SIGTERM, signal_term_handler)
