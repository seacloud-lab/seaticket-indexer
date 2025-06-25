import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from web_crawl_index.config import LOG_LEVEL, LOG_FILE


class LogConfigurator:
    def __init__(self, log_level=LOG_LEVEL, log_file=LOG_FILE, component_name=None):
        self.log_level = getattr(logging, log_level.upper())
        self.log_file = log_file
        self.component_name = component_name

        # Check for K8S environment
        is_k8s = os.getenv('KUBERNETES_SERVICE_HOST')

        handlers = []
        if is_k8s:
            # K8S: Log to stdout
            # The component_name will be part of the logger's name, e.g., logging.getLogger(component_name)
            log_format = f'[%(name)s] [%(asctime)s] [%(levelname)s] %(message)s'
            stream_handler = logging.StreamHandler(sys.stdout)
            handlers.append(stream_handler)
        else:
            # Non-K8S: Log to file
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
            log_format = '[%(asctime)s] [%(levelname)s] %(message)s'
            file_handler = RotatingFileHandler(
                self.log_file,
                maxBytes=10*1024*1024,  # 10MB
                backupCount=5
            )
            handlers.append(file_handler)

        logging.basicConfig(
            level=self.log_level,
            format=log_format,
            handlers=handlers,
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        self.logger = logging.getLogger(self.component_name if self.component_name else __name__)
        self.logger.info(f"Log configured, level: {log_level}, output: {'stdout' if is_k8s else log_file}")
