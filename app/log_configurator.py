import logging
import os
from logging.handlers import RotatingFileHandler
from config import LOG_LEVEL, LOG_FILE


class LogConfigurator:
    def __init__(self, log_level=LOG_LEVEL, log_file=LOG_FILE):
        self.log_level = getattr(logging, log_level.upper())
        self.log_file = log_file
        
        # Ensure log directory exists
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        
        # Configure root logger
        logging.basicConfig(
            level=self.log_level,
            format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            handlers=[
                RotatingFileHandler(
                    self.log_file, 
                    maxBytes=10*1024*1024,  # 10MB
                    backupCount=5
                ),
            ]
        )
        
        self.logger = logging.getLogger()
        self.logger.info(f"Log configured, level: {log_level}, file: {log_file}")
