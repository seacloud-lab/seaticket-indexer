import os
import sys
import yaml
import json
import logging

basedir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, basedir)
logger = logging.getLogger(__name__)

def _read_yaml(yaml_file_path=None, component_name=None):
    if yaml_file_path:
        if not os.path.isfile(yaml_file_path) or (not yaml_file_path.endswith('yml') and not yaml_file_path.endswith('yaml')):
            logger.warning(f'{yaml_file_path} is not existed or not a valid YAML file')
            return {}

        # first read, get vars from global
        configs = {}
        with open(yaml_file_path, 'r', encoding='utf-8') as yaml_file:
            current_yaml_config = yaml.safe_load(yaml_file) or {}
            configs = current_yaml_config.get('global', {})
            if component_name:
                component_config = current_yaml_config.get(component_name, {})
                configs.update(component_config)
                if 'from_yaml' in component_config:
                    del configs['from_yaml']
                    configs.update(_read_yaml(component_config['from_yaml']))
        return configs
    return {}

def _check_type(func):
    def wrapper(self, key, default=None, check_type=True):
        result = func(self, key, default)
        if check_type:
            need_type = type(default)
            if need_type in (int, float):
                try:
                    result = need_type(result)
                except:
                    raise ValueError(f'Type of {key} must be a number')
            elif need_type == bool:
                if isinstance(result, str):
                    result = result.lower() in ('true', '1')
                else:
                    result = bool(result)
            elif need_type in (str, list, dict):
                result = need_type(result)
        return result
    return wrapper

class _ConfigParser(object):
    def __init__(self, yaml_file_path, component_name):
        assert yaml_file_path and component_name, "yaml_file_path and component_name must be specified in initilizing ConfigParser"
        self.refresh_yaml_configs(yaml_file_path, component_name)

    def refresh_yaml_configs(self, yaml_file_path=None, component_name=None):
        self.yaml_file_path = yaml_file_path or self.yaml_file_path
        self.component_name = component_name or self.component_name
        try:
            self.yaml_configs = _read_yaml(self.yaml_file_path, self.component_name)
        except Exception as e:
            logger.error(f'Failure to read YAML config file: {e}')
            raise

    @_check_type
    def get(self, key, default=None):
        if key in os.environ:
            value = os.getenv(key)
            try:
                value = json.loads(value)
            except:
                pass
            return value
        return self.yaml_configs.get(key, default)
    
# init configs reader
CONF_DIR = os.environ.get('CONF_PATH', '/opt/seaqa/conf/')
yaml_file_path = os.path.join(CONF_DIR, os.environ.get('SEAQA_CONFIG_NAME', 'seaqa_config.yaml'))
configs = _ConfigParser(yaml_file_path, 'seaqa-indexer')

# Service info & logs
LOG_LEVEL = configs.get('LOG_LEVEL', 'info')
LOG_FILE = os.path.join(configs.get('LOG_PATH', '/tmp'), 'seaqa-indexer.log')

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
SEADB_SERVER_URL = configs.get('SEADB_SERVER_URL', 'http://seadb:8888')
SEADB_SERVER_ACCESS_TOKEN = configs.get('SEADB_SERVER_ACCESS_TOKEN', '')
SEADB_TABLE_SCHEMA_PATH = configs.get('SEADB_TABLE_SCHEMA_PATH', os.path.join(CONF_DIR, 'table_schemas.yaml'))

# SeaQA AI
SEAQA_AI_INNER_SERVER_URL = configs.get('SEAQA_AI_INNER_SERVER_URL', 'http://seaqa-ai:8887')

# index setting
LANG = configs.get('LANG', 'chinese')

SEASEARCH_URL = configs.get('SEASEARCH_URL', '')
SEASEARCH_TOKEN = configs.get('SEASEARCH_TOKEN', '')

SECRET_KEY = configs.get('SECRET_KEY', '')

JWT_PRIVATE_KEY = configs.get('JWT_PRIVATE_KEY', '')

GENERAL_TASK_STATUS_MAPPING = configs.get('GENERAL_TASK_STATUS_MAPPING', {
    'new': {'color': '#1A7F37', 'text_color': '#FFFFFF'},
    'in_progress': {'color': '#46A1FD', 'text_color': '#FFFFFF'},
    'done': {'color': '#8250DF', 'text_color': '#FFFFFF'},
    'canceled': {'color': '#59636E', 'text_color': '#FFFFFF'},
})

GENERAL_TASK_PRIORITY_MAPPING = configs.get('GENERAL_TASK_PRIORITY_MAPPING', {
    'high': {'color': '#FF6B6B', 'text_color': '#FFFFFF'},
    'medium': {'color': '#FFA500', 'text_color': '#FFFFFF'},
    'low': {'color': '#59CB74', 'text_color': '#FFFFFF'},
})

GENERAL_TASK_SIZE_MAPPING = configs.get('GENERAL_TASK_SIZE_MAPPING', {
    'large': {'color': '#FF6B6B', 'text_color': '#FFFFFF'},
    'medium': {'color': '#FFA500', 'text_color': '#FFFFFF'},
    'small': {'color': '#59CB74', 'text_color': '#FFFFFF'},
})

GENERAL_TASK_COLUMN_MAPPINGS = {
    'status': GENERAL_TASK_STATUS_MAPPING,
    'priority': GENERAL_TASK_PRIORITY_MAPPING,
    'size': GENERAL_TASK_SIZE_MAPPING,
}

REDIS_HOST = configs.get('REDIS_HOST', 'redis')
REDIS_PORT = configs.get('REDIS_PORT', 6379)
REDIS_PASSWORD = configs.get('REDIS_PASSWORD', '')

S3_HOST = configs.get('S3_HOST', '')
S3_FILE_BUCKET = configs.get('S3_FILE_BUCKET', '')
S3_WEB_CRAWL_BUCKET = configs.get('S3_WEB_CRAWL_BUCKET', '')
S3_KEY_ID = configs.get('S3_KEY_ID', '')
S3_SECRET_KEY = configs.get('S3_SECRET_KEY', '')

GITHUB_APP_ID = configs.get('GITHUB_APP_ID', '')
GITHUB_PRIVATE_KEY_PATH = configs.get('GITHUB_PRIVATE_KEY_PATH', '')

try:
    with open(GITHUB_PRIVATE_KEY_PATH, "r") as f:
        GITHUB_PRIVATE_KEY = f.read()
except:
    GITHUB_PRIVATE_KEY = ''
