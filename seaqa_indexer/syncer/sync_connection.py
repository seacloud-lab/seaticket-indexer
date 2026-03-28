import logging
import json
import datetime

from seaqa_indexer.utils.constants import CrawlStatus, SUPPORT_VECTOR_RETRIEVAL_TYPES
from seaqa_indexer.utils import decrypt_config, encrypt_config
from seaqa_indexer.utils.sync_logger import SyncLogger
from seaqa_indexer.handlers import get_handler

logger = logging.getLogger(__name__)


def sync_connection(connection_id, seaqa_db, mq, db_session_class):
    logger.info(f"Start sync connection: {connection_id}")
    sync_logger = SyncLogger(db_session_class).init(connection_id)
    sync_logger.log(f"Start sync connection: {connection_id}")
    project_connection = seaqa_db.get_project_connection_by_id(connection_id)
    if not project_connection:
        logger.warning('connection: %s not found', connection_id)
        sync_logger.log(f'connection: {connection_id} not found')
        sync_logger.stop_capture()
        return

    connection_is_active = project_connection.is_active
    if not connection_is_active:
        logger.warning('connection: %s is inactive', connection_id)
        sync_logger.log(f'connection: {connection_id} is inactive')
        sync_logger.stop_capture()
        return

    connection_status = json.loads(project_connection.status)

    connection_id = project_connection.id
    project_uuid = project_connection.project_uuid
    connection_type = project_connection.type
    old_last_sync_time = project_connection.last_sync_time
    connection_status['last_sync_status'] = CrawlStatus.CRAWLING
    seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))

    config = json.loads(project_connection.config)
    config = decrypt_config(config)

    try:
        conn_handler = get_handler(connection_type)
        if not conn_handler:
            logger.warning('Unsupported connection type: %s for connection %s', connection_type, connection_id)
            sync_logger.log(f'Unsupported connection type: {connection_type}')
            connection_status['last_sync_status'] = CrawlStatus.FAILED
            seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
            sync_logger.stop_capture()
            return

        conn_handler.mq = mq
        sync_count, total_count, is_update_config, config = conn_handler.sync(
            connection_id, project_uuid, config, old_last_sync_time,
            connection_status, seaqa_db, sync_logger
        )

        if sync_count is None:
            connection_status['last_sync_status'] = CrawlStatus.FAILED
            seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
            sync_logger.stop_capture()
            return

    except Exception as e:
        logger.exception('connection: %s, sync failed, error: %s', connection_id, e)
        try:
            if len(e.args) > 0:
                status_raw = e.args[0]
                status_text = str(status_raw)
                if '401' in status_text:
                    sync_logger.log('Sync failed: Unauthorized')
                elif '403' in status_text:
                    sync_logger.log('Sync failed: Forbidden')
                elif '404' in status_text:
                    err_msg = 'Page Not Found'
                    if len(e.args) > 1:
                        detail_raw = e.args[1]
                        try:
                            if isinstance(detail_raw, dict):
                                error = detail_raw
                            elif isinstance(detail_raw, str):
                                error = json.loads(detail_raw)
                            else:
                                error = {}
                            err_msg = error.get('error_msg', err_msg)
                        except Exception:
                            pass
                    sync_logger.log(f'Sync failed: {err_msg}')
                else:
                    sync_logger.log('Internal Server Error')
            else:
                sync_logger.log('Internal Server Error')
        except Exception as e:
            logger.error(e)
            sync_logger.log(f'Internal Server Error')
        connection_status['last_sync_status'] = CrawlStatus.FAILED
        seaqa_db.update_connection_status(connection_id, json.dumps(connection_status))
        sync_logger.stop_capture()
        return

    # finish sync
    connection_status['total_records'] = total_count
    connection_status['last_sync_count'] = sync_count
    connection_status['last_sync_status'] = CrawlStatus.COMPLETED
    last_sync_time = datetime.datetime.now(datetime.UTC)
    new_config = None
    if is_update_config:
        new_config = encrypt_config(config)
    seaqa_db.update_status_and_time(connection_id, json.dumps(connection_status), last_sync_time, config=new_config)

    try:
        sync_changes = connection_status.get('last_sync_changes') or {}
        connection_update_message = json.dumps({
            'project_uuid': project_uuid,
            'type': connection_type,
            'connection_id': int(connection_id),
            'added': int(sync_changes.get('added', 0) or 0),
            'deleted': int(sync_changes.get('deleted', 0) or 0),
            'updated': int(sync_changes.get('updated', sync_count or 0) or 0),
        })
        mq.publish('connection_update', connection_update_message)
    except Exception as e:
        logger.error('Failed to publish connection_update for project %s, connection %s: %s', project_uuid, connection_id, e)


    logger.info('Finish sync connection: %s, last sync count %s', connection_id, sync_count)
    sync_logger.log(f'Finish sync connection: {connection_id}')
    sync_logger.stop_capture()

    msg_content = {
        'project_uuid': project_uuid,
        'connection_id': connection_id,
        'type': connection_type
    }
    mq.lpush('index_task', json.dumps(msg_content))
    mq.lpush('ai_process_task', json.dumps(msg_content))
    if connection_type in SUPPORT_VECTOR_RETRIEVAL_TYPES:
        mq.lpush('content_vector_index_task', json.dumps({'connection_id': connection_id}))
