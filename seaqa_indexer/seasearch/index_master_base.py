import json
import time
import logging
from threading import Thread
from copy import deepcopy
from redis.exceptions import ConnectionError as NoMQAvailable, ResponseError, TimeoutError

from seaqa_indexer import config
from seaqa_indexer.mq import get_mq, NoMessageException

logger = logging.getLogger(__name__)


class IndexMasterBase(Thread):
    """
    Common index master class that listens on a Redis channel and enqueues tasks.

    Args:
        channel: Redis channel name to subscribe to.
        message_type: Identifier for the type of message received.
        task_queues: List of queue names that should receive the task.
    """
    def __init__(self, channel, message_type, task_queues):
        Thread.__init__(self)
        self.channel = channel
        self.message_type = message_type
        self.task_queues = task_queues if isinstance(task_queues, list) else [task_queues]
        self.mq = get_mq(config.REDIS_HOST, config.REDIS_PORT, config.REDIS_PASSWORD)
        self.pending_tasks = dict()
        self.interval = 10 * 60
        self.check_pending_tasks_interval = 30
        self.no_message_check_interval = 5 * 60

    def now(self):
        return time.time()

    def run(self):
        if not self.mq:
            return
        logger.info('%s index master event receive thread started', self.message_type)
        while True:
            try:
                self.master_handler()
            except NoMessageException:
                logger.warning('long time no message, reconnect to redis.')
            except (ResponseError, NoMQAvailable, TimeoutError) as e:
                logger.error('The connection to the redis server failed: %s' % e)
            except Exception as e:
                logger.error('Error handing master task: %s' % e)
            # prevent waste resource if redis or others didn't connectioned
            time.sleep(1)

    def master_handler(self):
        with self.mq.pubsub(ignore_subscribe_messages=True) as p:  # ensure pubsub connection is closed
            p.subscribe(self.channel)
            logger.info('%s master starting listen on channel: %s', self.message_type, self.channel)

            message_check_time = time.time()
            last_check_pending_tasks_time = time.time()
            while True:
                message = p.get_message(timeout=1.0)
                now_time = time.time()
                if message is not None and isinstance(message['data'], str):
                    message_check_time = time.time()
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
                            self._add_to_task_queue(project_uuid)

                if len(self.pending_tasks) and (now_time - last_check_pending_tasks_time) > self.check_pending_tasks_interval:
                    last_check_pending_tasks_time = time.time()
                    copied_pending_tasks = deepcopy(self.pending_tasks)
                    logger.info('start check pending tasks, task number: %s', len(copied_pending_tasks))
                    for project_uuid, last_update_time in copied_pending_tasks.items():
                        if (now_time - last_update_time) > self.interval:
                            self.pending_tasks.pop(project_uuid)
                            self._add_to_task_queue(project_uuid)
                
                if message is None:
                    if (time.time() - message_check_time) > self.no_message_check_interval:
                        raise NoMessageException
                    # prevent waste resource when no message has been send
                    time.sleep(1)

    def _add_to_task_queue(self, project_uuid):
        msg_data = json.dumps({
            'project_uuid': project_uuid,
            'type': self.message_type,
        })
        for queue in self.task_queues:
            self.mq.lpush(queue, msg_data)
        logger.info('%s has been add to task queue: %s', msg_data, ', '.join(self.task_queues))

