import os
import sys

basedir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, basedir)

LOG_LEVEL = 'info'
LOG_FILE = ''

HOST = '0.0.0.0'
PORT = 8888

# MySQL Configuration
MYSQL_HOST = ''
MYSQL_USER = ''
MYSQL_PASSWORD = ''
MYSQL_PORT = 3306
MYSQL_DB = 'sea_qa'
MYSQL_UNIX_SOCKET = ''

# index setting
LANG = 'chinese'

SEASEARCH_URL = ''
SEASEARCH_TOKEN = ''

SEAQA_WEB_SECRET_KEY = ''

try:
    from local_settings import *
except ImportError:
    pass

CONF_PATH = os.environ.get('CONF_PATH', '')
try:
    if os.path.exists(CONF_PATH):
        sys.path.insert(0, CONF_PATH)
    from seaqa_indexer_settings import *
except ImportError as e:
    pass


JWT_PRIVATE_KEY = os.environ.get('JWT_PRIVATE_KEY', '')

MYSQL_HOST = os.getenv('SEAQA_MYSQL_DB_HOST') or MYSQL_HOST
MYSQL_USER = os.getenv('SEAQA_MYSQL_DB_USER') or MYSQL_USER
MYSQL_PASSWORD = os.getenv('SEAQA_MYSQL_DB_PASSWORD') or MYSQL_PASSWORD
MYSQL_PORT = int(os.getenv('SEAQA_MYSQL_DB_PORT')) if os.getenv('WEB_CRAWLER_MYSQL_PORT') else MYSQL_PORT
MYSQL_DB_NAME = os.getenv('SEAQA_MYSQL_DB_NAME') or MYSQL_DB
MYSQL_UNIX_SOCKET = os.getenv('WEB_CRAWLER_MYSQL_UNIX_SOCKET') or MYSQL_UNIX_SOCKET

REDIS_HOST = os.environ.get('REDIS_HOST', 'redis')
REDIS_PORT = os.environ.get('REDIS_PORT', '6379')
REDIS_PASSWORD = os.environ.get('REDIS_PASSWORD', '')


# Crawler save directory
CRAWL_OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'crawled_content')
