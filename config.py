import os
from urllib.parse import quote_plus

# Log Configuration
LOG_LEVEL = 'INFO'
LOG_FILE = ''

# MySQL Configuration
MYSQL_HOST = ''
MYSQL_USER = ''
MYSQL_PASSWORD = ''
MYSQL_PORT = 3306
MYSQL_DB = 'crawl_web'
MYSQL_UNIX_SOCKET = ''

try:
    from local_settings import *
except ImportError:
    pass

MYSQL_HOST = os.getenv('WEB_CRAWLER_MYSQL_HOST') or MYSQL_HOST
MYSQL_USER = os.getenv('WEB_CRAWLER_MYSQL_USER') or MYSQL_USER
MYSQL_PASSWORD = os.getenv('WEB_CRAWLER_MYSQL_PASSWORD') or MYSQL_PASSWORD
MYSQL_PORT = int(os.getenv('WEB_CRAWLER_MYSQL_PORT')) if os.getenv('WEB_CRAWLER_MYSQL_PORT') else MYSQL_PORT
MYSQL_DB = os.getenv('WEB_CRAWLER_MYSQL_DB') or MYSQL_DB
MYSQL_UNIX_SOCKET = os.getenv('WEB_CRAWLER_MYSQL_UNIX_SOCKET') or MYSQL_UNIX_SOCKET


DATABASE_URI = "mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8" % \
                  (MYSQL_USER, quote_plus(MYSQL_PASSWORD), MYSQL_HOST, MYSQL_PORT, MYSQL_DB)

if MYSQL_UNIX_SOCKET:
    DATABASE_URI = DATABASE_URI + '&unix_socket=' + MYSQL_UNIX_SOCKET

# Crawler save directory
CRAWL_OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'crawled_content')
