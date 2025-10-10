import logging
from sqlalchemy import text
from seaqa_indexer.utils import uuid_str_to_32_chars


logger = logging.getLogger('__name__')


class SeaqaDB(object):
    def __init__(self, db_session_class):
        self.db_session_class = db_session_class

    def get_project_connection_by_id(self, connection_id):
        with self.db_session_class() as session:
            sql = """SELECT pc.id, pc.config, pc.project_uuid, pc.status, p.uuid as project_uuid, pc.last_sync_time, pc.type, pc.indexed_at, pc.is_active
            FROM project_connection pc 
            INNER JOIN projects p 
            ON p.uuid=pc.project_uuid WHERE p.deleted=false AND pc.id=:connection_id AND pc.deleted=false"""
            connection = session.execute(text(sql), {'connection_id': connection_id}).fetchone()
            return connection

    def get_connection_by_ids(self, project_uuid, connection_ids):
        project_uuid = uuid_str_to_32_chars(project_uuid)
        with self.db_session_class() as session:
            sql = """SELECT pc.id, pc.type, pc.config
            FROM project_connection pc 
            INNER JOIN projects p 
            ON p.uuid=pc.project_uuid WHERE p.deleted=false AND pc.deleted=false AND pc.project_uuid=:project_uuid"""

            params = {'project_uuid': project_uuid}
            if connection_ids:
                sql += " AND pc.id in :connection_ids"
                params['connection_ids'] = connection_ids

            return session.execute(text(sql), params).fetchall()

    def get_connections_by_project_uuid(self, project_uuid):
        project_uuid = uuid_str_to_32_chars(project_uuid)
        with self.db_session_class() as session:
            sql = """SELECT id, type FROM project_connection WHERE project_uuid=:project_uuid"""
            connection = session.execute(text(sql), {'project_uuid': project_uuid}).fetchall()
            return connection

    def update_connection_status(self, connection_id, status):
        with self.db_session_class() as session:
            cmd = "UPDATE project_connection SET status=:status WHERE id=:connection_id"
            session.execute(text(cmd), {'connection_id': connection_id, 'status': status})
            session.commit()

    def update_status_and_time(self, connection_id, status, last_sync_time):
        with self.db_session_class() as session:
            cmd = """UPDATE project_connection SET last_sync_time=:last_sync_time, status=:status WHERE id = :connection_id"""
            params = {'connection_id': connection_id, 'last_sync_time': last_sync_time, 'status': status}
            session.execute(text(cmd), params)
            session.commit()

    def update_connection_config(self, connection_id, config):
        with self.db_session_class() as session:
            cmd = """UPDATE project_connection SET config = :config WHERE id = :connection_id"""
            session.execute(text(cmd), {'connection_id': connection_id, 'config': config})
            session.commit()

    def update_connection(self, connection_id, indexed_at, connection_status, last_sync_time=None, config=None):
        with self.db_session_class() as session:
            cmd = """UPDATE project_connection SET indexed_at=:indexed_at, status=:connection_status"""
            params = {'connection_id': connection_id, 'indexed_at': indexed_at, 'connection_status': connection_status}
            if last_sync_time:
                cmd += ", last_sync_time=:last_sync_time"
                params['last_sync_time'] = last_sync_time
            if config:
                cmd += ", config=:config"
                params['config'] = config
            cmd += " WHERE id = :connection_id"
            session.execute(text(cmd), params)
            session.commit()

    def get_connections_to_recover(self, need_recover_sync_types):
        with self.db_session_class() as session:
            need_sync_types_combine_str = ','.join([f"'{t}'" for t in need_recover_sync_types])
            sql = f"""SELECT id FROM `project_connection` 
                     WHERE (JSON_UNQUOTE(JSON_EXTRACT(`status`, '$.last_sync_status')) in ('crawling', 'pending')
                      OR JSON_UNQUOTE(JSON_EXTRACT(`status`, '$.last_index_status')) = 'indexing') 
                     AND deleted=false 
                     AND is_active=true 
                     AND type IN ({need_sync_types_combine_str})"""

            return session.execute(text(sql)).fetchall()
