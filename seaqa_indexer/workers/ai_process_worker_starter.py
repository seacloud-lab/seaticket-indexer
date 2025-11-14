# coding: UTF-8
import logging
from threading import Thread, Event

from seaqa_indexer.config import LOG_FILE, LOG_LEVEL
from seaqa_indexer.utils import get_python_executable, run


__all__ = [
    'AIProcessWorkerStarter',
]


class AIProcessWorkerStarter(object):
    def __init__(self):
        self._enabled = True
        self._logfile = LOG_FILE
        self._loglevel = LOG_LEVEL

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start ai process worker starter: it is not enabled!')
            return

        AIProcessWorker(self._logfile, self._loglevel).start()
        logging.info('ai process worker starter is started')

    def is_enabled(self):
        return self._enabled


class AIProcessWorker(Thread):
    def __init__(self, logfile, loglevel):
        Thread.__init__(self)
        self._logfile = logfile
        self._loglevel = loglevel
        self.finished = Event()

    def run(self):
        try:
            cmd = [
                get_python_executable(),
                '-m', 'seaqa_indexer.ai_processor.ai_process_task_worker'
            ]
            run(cmd, )
        except Exception as e:
            logging.exception('error when start ai process worker: %s', e)

    def cancel(self):
        self.finished.set()
