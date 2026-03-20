import sys
import time
import logging
import json
import threading
import signal
from abc import ABC, abstractmethod
from redis.exceptions import ConnectionError as NoMQAvailable, ResponseError, TimeoutError

from seaqa_indexer import config
from seaqa_indexer.app.log import LogConfigurator
from seaqa_indexer.mq import get_mq


logger = logging.getLogger(__name__)


class BaseTaskWorker(ABC):
    def __init__(self):
        self.should_stop = threading.Event()
        self.worker_list = []
        self.mq = get_mq(config.REDIS_HOST, config.REDIS_PORT, config.REDIS_PASSWORD)
        self.init_dependencies()
        self.set_signal()

    @property
    def worker_num(self) -> int:
        return 3

    @property
    def tname(self):
        return threading.current_thread().name

    @abstractmethod
    def get_queue_name(self) -> str:
        pass

    @abstractmethod
    def get_worker_name(self) -> str:
        pass

    @abstractmethod
    def process_message(self, data: dict):
        pass

    def init_dependencies(self):
        pass

    def clear_worker(self):
        for th in self.worker_list:
            th.join()
        logger.info("All %s threads has stopped.", self.get_worker_name())

    def start(self):
        if not self.mq:
            return
        self.on_start()
        for i in range(int(self.worker_num)):
            t = threading.Thread(
                target=self.task_worker,
                name=f'{self.get_worker_name()}_thread_{i}',
                daemon=True
            )
            t.start()
            self.worker_list.append(t)

    def on_start(self):
        """additional startup operations"""
        pass

    def task_worker(self):
        logger.info('%s starting %s', self.tname, self.get_worker_name())
        while not self.should_stop.is_set():
            try:
                res = self.mq.brpop(self.get_queue_name(), timeout=30)
                if res is not None:
                    _, value = res
                    try:
                        data = json.loads(value)
                    except:
                        data = None
                    if not data:
                        logger.warning('%s: invalid message.', self.get_queue_name(), res)
                    else:
                        self.process_message(data)
            except (ResponseError, NoMQAvailable, TimeoutError) as e:
                logger.error('The connection to the redis server failed: %s' % e)
                time.sleep(1)
            except Exception as e:
                logger.exception('%s %s Error: %s', self.tname, self.get_worker_name(), e)
                logger.error(e, exc_info=True)
                time.sleep(1)

    def clear(self):
        if not self.mq:
            return
        self.should_stop.set()
        time.sleep(1)
        self.clear_lock()
        logger.info("Exit %s process", self.get_worker_name())
        sys.exit(0)

    def clear_lock(self):
        """additional cleanup operations"""
        pass

    def signal_term_handler(self, signal, frame):
        self.clear()

    def set_signal(self):
        # TODO: look like python will add signal to queue when cpu exec c extension code,
        # and will call signal callback method after cpu exec python code
        # ref: https://docs.python.org/2/library/signal.html
        signal.signal(signal.SIGTERM, self.signal_term_handler)


class LockingTaskWorker(BaseTaskWorker):
    """Base class for task workers with Redis distributed locks"""

    LOCK_TIMEOUT = 1800  # 30 minutes
    REFRESH_INTERVAL = 600  # 10 minutes

    def __init__(self):
        self.locked_keys = set()
        self.locked_keys_lock = threading.Lock()
        super().__init__()

    @abstractmethod
    def get_lock_key(self, task_id) -> str:
        pass

    def on_start(self):
        """start the lock refresh thread"""
        threading.Thread(target=self.refresh_lock, name='refresh_thread', daemon=True).start()

    def execute_with_lock(self, task_id, task_func, *args, **kwargs):
        """execute task with distributed lock

        Args:
            task_id: task identifier, used to generate lock key
            task_func: task function to execute
            *args: positional arguments to pass to the task function
            **kwargs: keyword arguments to pass to the task function
        """
        lock_key = self.get_lock_key(task_id)
        if self.mq.set(lock_key, time.time(), ex=self.LOCK_TIMEOUT, nx=True):
            logger.info('%s start %s: %s', self.tname, self.get_worker_name(), task_id)
            with self.locked_keys_lock:
                self.locked_keys.add(lock_key)
            try:
                task_func(*args, **kwargs)
            except Exception as e:
                logger.exception('%s %s failed, error: %s', self.get_worker_name(), task_id, e)
            finally:
                try:
                    with self.locked_keys_lock:
                        self.locked_keys.remove(lock_key)
                except KeyError:
                    logger.error("%s is already removed. SHOULD NOT HAPPEN!" % lock_key)
                self.mq.delete(lock_key)
            logger.info('%s Finish %s: %s, delete redis lock %s',
                        self.tname, self.get_worker_name(), task_id, lock_key)
        else:
            logger.warning('another %s task is running, drop this task: %s', self.get_worker_name(), task_id)

    def refresh_lock(self):
        """periodically refresh the expiration time of the lock"""
        logger.info('%s Starting refresh locks', self.tname)
        while not self.should_stop.is_set():
            try:
                with self.locked_keys_lock:
                    copy = self.locked_keys.copy()
                for lock in copy:
                    ttl = self.mq.ttl(lock)
                    new_ttl = ttl + self.REFRESH_INTERVAL
                    self.mq.expire(lock, new_ttl)
                    logger.debug('%s Refresh lock [%s] timeout from %s to %s',
                                 self.tname, lock, ttl, new_ttl)
                time.sleep(self.REFRESH_INTERVAL)
            except Exception as e:
                logger.exception(e)
                time.sleep(1)

    def clear_lock(self):
        """clean all held locks"""
        with self.locked_keys_lock:
            keys_to_delete = list(self.locked_keys)
        for key in keys_to_delete:
            self.mq.delete(key)
            logger.info("redis lock key %s has been deleted", key)


def run_worker(worker_class, log_level=None, log_file=None):
    """generic worker startup function

    Args:
        worker_class: Worker class
        log_level: log level, default is config.LOG_LEVEL
        log_file: log file, default is config.LOG_FILE
    """
    LogConfigurator(log_level or config.LOG_LEVEL, log_file or config.LOG_FILE)
    worker = worker_class()
    logger.info("%s process initialized.", worker.get_worker_name())
    try:
        worker.start()
    except Exception as e:
        logger.exception(e)
        worker.clear()
        sys.exit(1)

    while True:
        time.sleep(2)
