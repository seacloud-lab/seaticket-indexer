# coding: UTF-8
import logging
import json
from threading import Thread, Event

from seaqa_indexer.config import LOG_FILE, LOG_LEVEL, REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, KB_AI_SUMMARY_TASK_INTERVAL
from seaqa_indexer.db import init_db_session_class
from seaqa_indexer.utils.seaqa_db import SeaqaDB
from seaqa_indexer.mq import get_mq


logger = logging.getLogger(__name__)

__all__ = [
    'KBSummaryTaskPublish',
]


class KBSummaryTaskPublish(object):
    def __init__(self):
        self._enabled = True
        self._logfile = LOG_FILE
        self._loglevel = LOG_LEVEL
        self._interval = KB_AI_SUMMARY_TASK_INTERVAL

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start knowledge base AI summary task publisher: it is not enabled!')
            return

        KBSummaryTaskPublisherTimer(self._logfile, self._loglevel, self._interval).start()
        logging.info('knowledge base AI summary task publisher is started')

    def is_enabled(self):
        return self._enabled


class KBSummaryTaskPublisherTimer(Thread):
    def __init__(self, logfile, loglevel, interval):
        super(KBSummaryTaskPublisherTimer, self).__init__()
        self._logfile = logfile
        self._loglevel = loglevel
        self.db_session_class = init_db_session_class()
        self.mq = get_mq(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)
        self.seaqa_db = SeaqaDB(self.db_session_class)
        self.finished = Event()
        self._interval = interval

    def publish_ai_summary_tasks(self):
        try:
            projects = self.seaqa_db.get_all_active_projects()
            for p in projects:
                project_uuid = getattr(p, 'uuid', None)
                if not project_uuid:
                    continue
                try:
                    self.add_ai_summary_task(project_uuid)
                except Exception as e:
                    logger.error('publish ai summary task failed, project_uuid: %s, error: %s', project_uuid, e)
        except Exception as e:
            logger.exception('get active projects error: %s', e)

    def add_ai_summary_task(self, project_uuid):
        msg_content = {
            'project_uuid': project_uuid,
            'type': 'knowledge_base',
        }
        self.mq.lpush('ai_process_task', json.dumps(msg_content))

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self._interval)
            if self.finished.is_set():
                break
            logging.info('starts to publish knowledge base AI summary tasks')
            try:
                self.publish_ai_summary_tasks()
            except Exception as e:
                logging.exception('publish knowledge base AI summary tasks error: %s', e)

    def cancel(self):
        self.finished.set()
