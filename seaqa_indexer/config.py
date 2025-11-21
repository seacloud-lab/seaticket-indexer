import os
import sys
import yaml
import logging

basedir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, basedir)
logger = logging.getLogger(__name__)

import os
import yaml
import logging

logger = logging.getLogger(__name__)
def _read_yaml(yaml_file_path=None, component_name=None):
    if yaml_file_path:
        # first read, get vars from global
        configs = {}
        try:
            with open(yaml_file_path, 'r', encoding='utf-8') as yaml_file:
                current_yaml_config = yaml.safe_load(yaml_file)
                configs = current_yaml_config.get('global', {})
                if component_name:
                    component_config = current_yaml_config.get(component_name, {})
                    configs.update(component_config)
                    if 'from_yaml' in component_config:
                        del configs['from_yaml']
                        configs.update(_read_yaml(component_config['from_yaml']))
        except Exception as e:
            logger.error(f'Failure to read YAML config file: {e}')
        return configs
    return {}

def _check_number(func):
    def wrapper(self, key, default=None, check_number=True):
        result = func(self, key, default)
        if check_number and (need_type := type(default)) in (int, float):
            try:
                result = need_type(result)
            except:
                raise ValueError(f'Type of {key} must be a number')
        return result
    return wrapper

class _ConfigParser(object):
    def __init__(self, yaml_file_path, component_name):
        assert yaml_file_path and component_name, "yaml_file_path and component_name must be specified in initilizing ConfigParser"
        self.refresh_yaml_configs(yaml_file_path, component_name)
    
    def refresh_yaml_configs(self, yaml_file_path=None, component_name=None):
        self.yaml_file_path = yaml_file_path or self.yaml_file_path
        self.component_name = component_name or self.component_name
        self.yaml_configs = _read_yaml(self.yaml_file_path, self.component_name)
    
    @_check_number
    def get(self, key, default=None):
        
        return os.getenv(key) if key in os.environ else self.yaml_configs.get(key, default)

# init configs reader
CONF_DIR = os.environ.get('CONF_PATH', '/opt/seaqa/conf/')
yaml_file_path = os.path.join(CONF_DIR, os.environ.get('SEAQA_CONFIG_NAME', 'seaqa_config.yaml'))
configs = _ConfigParser(yaml_file_path, 'seaqa-indexer')

# Service info & logs
LOG_LEVEL = configs.get('SEAQA_INDEXER_LOG_LEVEL', 'info')
LOG_FILE = configs.get('SEAQA_INDEXER_LOG_FILE', 'seaqa-indexer.log')

HOST = configs.get('SEAQA_INDEXER_LISTEN_HOST', '0.0.0.0')
PORT = configs.get('SEAQA_INDEXER_LISTEN_PORT', 8888)

# MySQL Configuration
MYSQL_HOST = configs.get('SEAQA_MYSQL_DB_HOST', 'db')
MYSQL_USER = configs.get('SEAQA_MYSQL_DB_USER', 'seaqa')
MYSQL_PASSWORD = configs.get('SEAQA_MYSQL_DB_PASSWORD', '')
MYSQL_PORT = configs.get('SEAQA_MYSQL_DB_PORT', 3306)
MYSQL_DB_NAME = configs.get('SEAQA_MYSQL_SEAQA_DB_NAME', 'sea_qa')
MYSQL_UNIX_SOCKET = os.getenv('WEB_CRAWLER_MYSQL_UNIX_SOCKET', '')

# SeaDB Configuration
SEADB_INNER_SERVER_URL = configs.get('SEADB_INNER_SERVER_URL', '')

# SeaQA AI
SEAQA_AI_INNER_SERVER_URL = configs.get('SEAQA_AI_INNER_SERVER_URL', '')

# index setting
LANG = configs.get('LANG', 'chinese')

SEASEARCH_URL = configs.get('SEASEARCH_URL', '')
SEASEARCH_TOKEN = configs.get('SEASEARCH_TOKEN', '')

SEAQA_WEB_SECRET_KEY = configs.get('SEAQA_WEB_SECRET_KEY', '')

JWT_PRIVATE_KEY = configs.get('JWT_PRIVATE_KEY', '')

REDIS_HOST = configs.get('REDIS_HOST', 'redis')
REDIS_PORT = configs.get('REDIS_PORT', 6379)
REDIS_PASSWORD = configs.get('REDIS_PASSWORD', '')

S3_HOST = configs.get('S3_HOST')
S3_WEB_CRAWL_BUCKET = configs.get('S3_WEB_CRAWL_BUCKET')
S3_KEY_ID = configs.get('S3_KEY_ID')
S3_SECRET_KEY = configs.get('S3_SECRET_KEY')
