import logging
from urllib.parse import quote_plus

from sqlalchemy import create_engine
from sqlalchemy.event import contains as has_event_listener, listen as add_event_listener
from sqlalchemy.exc import DisconnectionError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import Pool

from seaqa_indexer.config import MYSQL_DB_NAME, MYSQL_HOST, MYSQL_PORT, MYSQL_PASSWORD, MYSQL_USER


logger = logging.getLogger(__name__)


def create_engine_from_env():
    '''
    Basicly, there are 3 different databses in a mysql-server involved in all seafile project.
    seahub_db, seafile_db, ccnet_db which are assigned in .env file.

    :param db:  The name of database
    :return:  An engine by which can make db sessions
    '''
    need_connection_pool_fix = True

    db_name = MYSQL_DB_NAME
    db_host = MYSQL_HOST
    db_port = MYSQL_PORT
    db_user = MYSQL_USER
    db_pwd = MYSQL_PASSWORD

    if not (db_name and db_host and db_port and db_user):
        raise RuntimeError('Database configured error')

    db_url = "mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8" % (db_user, quote_plus(db_pwd), db_host, db_port, db_name)
    kwargs = dict(pool_recycle=300, echo=False, echo_pool=False)

    engine = create_engine(db_url, **kwargs)

    if need_connection_pool_fix and not has_event_listener(Pool, 'checkout', ping_connection):
        # We use has_event_listener to double check in case we call create_engine
        # multipe times in the same process.
        add_event_listener(Pool, 'checkout', ping_connection)

    return engine


def init_db_session_class():
    """Configure Session class for mysql according to the env."""
    try:
        engine = create_engine_from_env()
    except Exception as e:
        logger.error(e)
        raise RuntimeError("create db engine error: %s" % e)

    Session = sessionmaker(bind=engine)
    return Session


# This is used to fix the problem of "MySQL has gone away" that happens when
# mysql server is restarted or the pooled connections are closed by the mysql
# server beacause being idle for too long.
#
# See http://stackoverflow.com/a/17791117/1467959
def ping_connection(dbapi_connection, connection_record, connection_proxy): # pylint: disable=unused-argument
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("SELECT 1")
        cursor.close()
    except:
        logger.info('fail to ping database server, disposing all cached connections')
        connection_proxy._pool.dispose() # pylint: disable=protected-access

        # Raise DisconnectionError so the pool would create a new connection
        raise DisconnectionError()
