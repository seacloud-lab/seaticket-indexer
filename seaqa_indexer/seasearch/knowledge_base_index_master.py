import json
import time
import logging
from threading import Thread
from copy import deepcopy
from redis.exceptions import ConnectionError as NoMQAvailable, ResponseError, TimeoutError

from seaqa_indexer import config
from seaqa_indexer.mq import get_mq

logger = logging.getLogger(__name__)


class KnowledgeBaseIndexMaster(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.mq = get_mq(config.REDIS_HOST, config.REDIS_PORT, config.REDIS_PASSWORD)
        self.pending_tasks = dict()
        self.interval = 10 * 60

    def now(self):
        return time.time()

    def run(self):
        if not self.mq:
            return
        logger.info('knowledge base index master event receive thread started')
        while True:
            try:
                self.master_handler()
            except (ResponseError, TimeoutError) as e:
                logger.error('The connection to the redis server failed: %s' % e)
            except NoMQAvailable as e:
                logger.error('no MQ available failed: %s' % e)
            except Exception as e:
                logger.error('Error handing master task: %s' % e)
            #prevent waste resource if redis or others didn't connectioned
            time.sleep(1)

    def master_handler(self):
        p = self.mq.pubsub(ignore_subscribe_messages=True)
        p.subscribe('knowledge_base_update')
        logger.info('knowledge_base master starting listen')

        last_check_pending_tasks_time = time.time()
        while True:
            message = p.get_message()
            now_time = time.time()
            if message is not None and isinstance(message['data'], str):
                try:
                    data = json.loads(message['data'])
                except:
                    logger.warning('message: invalid.', message)
                    data = None
                if data:
                    project_uuid = data.get('project_uuid')

                    last_update_time = self.pending_tasks.get(project_uuid, 0)
                    if (now_time - last_update_time) > self.interval:
                        self.pending_tasks[project_uuid] = now_time
                        msg_data = json.dumps({
                            'project_uuid': project_uuid,
                            'type': 'knowledge_base',
                        })
                        self.mq.lpush('index_task', msg_data)
                        logger.info('%s has been add to task queue' % msg_data)

            if len(self.pending_tasks) and (now_time - last_check_pending_tasks_time) > 30:
                last_check_pending_tasks_time = time.time()
                copied_pending_tasks = deepcopy(self.pending_tasks)
                logger.info('start check pending tasks, task number: %s', len(copied_pending_tasks))
                for project_uuid, last_update_time in copied_pending_tasks.items():
                    if (now_time - last_update_time) > self.interval:
                        self.pending_tasks.pop(project_uuid)
                        msg_data = json.dumps({
                            'project_uuid': project_uuid,
                            'type': 'knowledge_base',
                        })
                        self.mq.lpush('index_task', msg_data)
                        logger.info('pending task %s has been add to task queue' % msg_data)

            if message is None:
                # prevent waste resource when no message has been send
                time.sleep(1)
