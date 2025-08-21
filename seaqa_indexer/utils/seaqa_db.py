import logging
from sqlalchemy import text


logger = logging.getLogger('__name__')


class SeaqaDB(object):
    def __init__(self, db_session_class):
        self.db_session_class = db_session_class

    def get_project_connection_by_id(self, connection_id):
        with self.db_session_class() as session:
            sql = """SELECT pc.id, pc.config, pc.project_id, pc.status, p.uuid as project_uuid, pc.updated_at, pc.type
            FROM project_connection pc 
            INNER JOIN projects p 
            ON p.id=pc.project_id WHERE p.deleted=false AND pc.id=:connection_id"""
            connection = session.execute(text(sql), {'connection_id': connection_id}).fetchone()
            return connection

    def update_connection_status(self, connection_id, status):
        with self.db_session_class() as session:
            cmd = "UPDATE project_connection SET status = :status WHERE id = :connection_id"
            session.execute(text(cmd), {'connection_id': connection_id, 'status': status})
            session.commit()

    def update_connection_updated_at(self, connection_id, updated_at):
        """Update connection's updated_at timestamp"""
        with self.db_session_class() as session:
            sql = """UPDATE project_connection
                       SET updated_at = :updated_at
                       WHERE id = :connection_id"""
            session.execute(text(sql), {
                'updated_at': updated_at,
                'connection_id': connection_id
            })
            session.commit()

    def update_status_and_time(self, connection_id, status, updated_at):
        with self.db_session_class() as session:
            cmd = """UPDATE project_connection SET updated_at=:updated_at, status=:status WHERE id = :connection_id"""
            params = {'connection_id': connection_id, 'updated_at': updated_at, 'status': status}
            session.execute(text(cmd), params)
            session.commit()

    def update_connection_config(self, connection_id, config):
        with self.db_session_class() as session:
            cmd = """UPDATE project_connection SET config = :config WHERE id = :connection_id"""
            session.execute(text(cmd), {'connection_id': connection_id, 'config': config})
            session.commit()
