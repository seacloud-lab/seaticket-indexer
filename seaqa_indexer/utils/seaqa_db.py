import logging
import json
from sqlalchemy import text
from seaqa_indexer.utils import uuid_str_to_32_chars, uuid_str_to_36_chars
from seaqa_indexer.utils.constants import ConnectionType, CrawlStatus, SUPPORT_VECTOR_RETRIEVAL_TYPES, IndexStatus, AIStatus


logger = logging.getLogger('__name__')


class SeaqaDB(object):
    def __init__(self, db_session_class):
        self.db_session_class = db_session_class

    def get_project_connections_by_installation_id(self, installation_id):
        with self.db_session_class() as session:
            sql = """SELECT pc.id, pc.config, pc.project_uuid, pc.status, p.uuid as project_uuid, pc.last_sync_time, pc.type, pc.is_active
            FROM project_connection pc
            INNER JOIN projects p
            ON p.uuid=pc.project_uuid WHERE p.deleted=false AND pc.deleted=false AND
            JSON_UNQUOTE(JSON_EXTRACT(`config`, '$.installation_id')) = :installation_id
            """
            connections = session.execute(
                text(sql), {'installation_id': installation_id}
            ).fetchall()
            return connections

    def get_project_linear_oauth(self, project_uuid):
        project_uuid = uuid_str_to_32_chars(project_uuid)
        with self.db_session_class() as session:
            sql = """SELECT access_token, refresh_token, expires_at 
                     FROM project_linear_oauth 
                     WHERE project_uuid=:project_uuid 
                     """
            row = session.execute(text(sql), {'project_uuid': project_uuid}).fetchone()
            return row

    def get_project_deadline_linear_oauth(self, deadline_time):
        with self.db_session_class() as session:
            sql = """SELECT access_token, refresh_token, expires_at, project_uuid
                     FROM project_linear_oauth 
                     WHERE expires_at < :deadline_time
                     """
            rows = session.execute(text(sql), {'deadline_time': deadline_time}).fetchall()
            return rows

    def update_project_linear_oauth(self, project_uuid, access_token, expires_at, refresh_token):
        project_uuid = uuid_str_to_32_chars(project_uuid)
        with self.db_session_class() as session:
            cmd = """UPDATE project_linear_oauth
                        SET access_token=:access_token, refresh_token=:refresh_token, expires_at=:expires_at
                        WHERE project_uuid=:project_uuid"""
            session.execute(text(cmd), {
                'project_uuid': project_uuid,
                'access_token': access_token,
                'refresh_token': refresh_token,
                'expires_at': expires_at,
            })
            session.commit()

    def get_project_jira_oauth(self, project_uuid):
        project_uuid = uuid_str_to_32_chars(project_uuid)
        with self.db_session_class() as session:
            sql = """SELECT access_token, refresh_token, expires_at
                     FROM project_jira_oauth
                     WHERE project_uuid=:project_uuid
                     """
            row = session.execute(text(sql), {'project_uuid': project_uuid}).fetchone()
            return row

    def get_jira_oauth_page(self, offset=0, limit=100):
        with self.db_session_class() as session:
            sql = """
                SELECT project_uuid, access_token, refresh_token, expires_at
                FROM project_jira_oauth
                ORDER BY project_uuid
                LIMIT :limit OFFSET :offset
            """
            return session.execute(
                text(sql), {'limit': limit, 'offset': offset}
            ).fetchall()

    def update_project_jira_oauth(self, project_uuid, access_token, expires_at, refresh_token):
        project_uuid = uuid_str_to_32_chars(project_uuid)
        with self.db_session_class() as session:
            cmd = """UPDATE project_jira_oauth
                        SET access_token=:access_token, refresh_token=:refresh_token, expires_at=:expires_at
                        WHERE project_uuid=:project_uuid"""
            session.execute(text(cmd), {
                'project_uuid': project_uuid,
                'access_token': access_token,
                'refresh_token': refresh_token,
                'expires_at': expires_at,
            })
            session.commit()

    def get_confluence_connection_by_workspace_id(self, workspace_id):
        with self.db_session_class() as session:
            sql = """SELECT pc.id, pc.config, pc.project_uuid, pc.status, p.uuid as project_uuid, pc.last_sync_time, pc.type, pc.is_active
            FROM project_connection pc
            INNER JOIN projects p
            ON p.uuid=pc.project_uuid WHERE p.deleted=false AND pc.deleted=false AND
            pc.type = 'confluence' AND
            JSON_UNQUOTE(JSON_EXTRACT(`config`, '$.workspace_id')) = :workspace_id
            """
            connections = session.execute(
                text(sql), {'workspace_id': workspace_id}
            ).fetchall()
            return connections

    def get_project_connection_by_id(self, connection_id):
        with self.db_session_class() as session:
            sql = """SELECT pc.id, pc.config, pc.project_uuid, pc.status, pc.ai_status, pc.content_vector_status, p.uuid as project_uuid, pc.last_sync_time, pc.type, pc.indexed_at, pc.ai_indexed_at, pc.content_vector_indexed_at, pc.is_active, pc.created_at
            FROM project_connection pc 
            INNER JOIN projects p 
            ON p.uuid=pc.project_uuid WHERE p.deleted=false AND pc.id=:connection_id AND pc.deleted=false"""
            connection = session.execute(
                text(sql), {'connection_id': connection_id}
            ).fetchone()
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
            sql = """SELECT id, type, config, deleted, is_active
                     FROM project_connection
                     WHERE project_uuid=:project_uuid"""
            connection = session.execute(text(sql), {'project_uuid': project_uuid}).fetchall()
            return connection

    def get_project_connections_by_type(self, project_uuid, connection_type):
        """Get active, non-deleted connections of a specific type for a project."""
        project_uuid = uuid_str_to_32_chars(project_uuid)
        with self.db_session_class() as session:
            sql = """SELECT id, type, config
                     FROM project_connection
                     WHERE project_uuid = :project_uuid
                       AND type = :connection_type"""
            return session.execute(text(sql), {
                'project_uuid': project_uuid,
                'connection_type': connection_type,
            }).fetchall()
    
    def set_connection_pending_status(self, connection_id, status, connection_type):
        connection_status = json.dumps({
            "last_indexed_count": 0, 
            "last_index_status": IndexStatus.PENDING, 
            "total_records": json.loads(status).get('total_records'), 
            "last_sync_count": 0, 
            "last_sync_status": CrawlStatus.PENDING
        })

        ai_status = json.dumps({
            "last_ai_processing_count": 0, 
            "last_ai_processing_status": AIStatus.PENDING, 
            "last_ai_index_status": IndexStatus.PENDING, 
            "last_ai_indexed_count": 0
        })

        with self.db_session_class() as session:
            cmd = "UPDATE project_connection SET status=:status, ai_status=:ai_status"
            params = {'connection_id': connection_id, 'status': connection_status, 'ai_status': ai_status}

            if connection_type in SUPPORT_VECTOR_RETRIEVAL_TYPES:
                content_vector_status = json.dumps({
                    "last_content_vector_index_status": IndexStatus.PENDING, 
                    "last_content_vector_indexed_count": 0
                })
                cmd += ", content_vector_status=:content_vector_status"
                params['content_vector_status'] = content_vector_status
            
            cmd += " WHERE id=:connection_id"
            session.execute(text(cmd), params)
            session.commit()
    
    def set_connection_crawling_status(self, connection_id, status, connection_type):
        connection_status = json.dumps({
            "last_indexed_count": 0, 
            "last_index_status": IndexStatus.PENDING, 
            "total_records": status.get('total_records'), 
            "last_sync_count": 0, 
            "last_sync_status": CrawlStatus.CRAWLING
        })

        ai_status = json.dumps({
            "last_ai_processing_count": 0, 
            "last_ai_processing_status": AIStatus.PENDING, 
            "last_ai_index_status": IndexStatus.PENDING, 
            "last_ai_indexed_count": 0
        })

        with self.db_session_class() as session:
            cmd = "UPDATE project_connection SET status=:status, ai_status=:ai_status"
            params = {'connection_id': connection_id, 'status': connection_status, 'ai_status': ai_status}

            if connection_type in SUPPORT_VECTOR_RETRIEVAL_TYPES:
                content_vector_status = json.dumps({
                    "last_content_vector_index_status": IndexStatus.PENDING, 
                    "last_content_vector_indexed_count": 0
                })
                cmd += ", content_vector_status=:content_vector_status"
                params['content_vector_status'] = content_vector_status
            
            cmd += " WHERE id=:connection_id"
            session.execute(text(cmd), params)
            session.commit()
        return json.loads(connection_status)

    def update_connection_status(self, connection_id, status):
        with self.db_session_class() as session:
            cmd = "UPDATE project_connection SET status=:status WHERE id=:connection_id"
            session.execute(text(cmd), {'connection_id': connection_id, 'status': status})
            session.commit()

    def update_status_and_time(self, connection_id, status, last_sync_time, config=None):
        with self.db_session_class() as session:
            cmd = """UPDATE project_connection SET last_sync_time=:last_sync_time, status=:status"""
            params = {'connection_id': connection_id, 'last_sync_time': last_sync_time, 'status': status}
            if config:
                cmd += ", config=:config"
                params['config'] = config
            cmd += " WHERE id = :connection_id"
            session.execute(text(cmd), params)
            session.commit()

    def update_ai_status_and_time(self, connection_id, ai_status, last_ai_processing_time=None):
        with self.db_session_class() as session:
            cmd = """UPDATE project_connection SET ai_status=:ai_status"""
            params = {'connection_id': connection_id, 'ai_status': ai_status}
            if last_ai_processing_time:
                cmd += ", last_ai_processing_time=:last_ai_processing_time"
                params['last_ai_processing_time'] = last_ai_processing_time
            cmd += " WHERE id = :connection_id"
            session.execute(text(cmd), params)
            session.commit()

    def update_connection_config(self, connection_id, config):
        with self.db_session_class() as session:
            cmd = """UPDATE project_connection SET config = :config WHERE id = :connection_id"""
            session.execute(text(cmd), {'connection_id': connection_id, 'config': config})
            session.commit()

    def update_connection(self, connection_id, indexed_at, connection_status):
        with self.db_session_class() as session:
            cmd = """UPDATE project_connection SET indexed_at=:indexed_at, status=:connection_status"""
            params = {'connection_id': connection_id, 'indexed_at': indexed_at, 'connection_status': connection_status}
            cmd += " WHERE id = :connection_id"
            session.execute(text(cmd), params)
            session.commit()

    def update_ai_index_status(self, connection_id, ai_status):
        """Update AI index status"""
        with self.db_session_class() as session:
            cmd = """UPDATE project_connection SET ai_status=:ai_status WHERE id=:connection_id"""
            session.execute(text(cmd), {'connection_id': connection_id, 'ai_status': ai_status})
            session.commit()

    def update_ai_indexed_at_and_status(self, connection_id, ai_indexed_at, ai_status):
        """Update AI indexed time and status"""
        with self.db_session_class() as session:
            cmd = """UPDATE project_connection SET ai_indexed_at=:ai_indexed_at, ai_status=:ai_status WHERE id=:connection_id"""
            session.execute(text(cmd), {'connection_id': connection_id, 'ai_indexed_at': ai_indexed_at, 'ai_status': ai_status})
            session.commit()

    def update_content_vector_status_and_time(self, connection_id, content_vector_status, last_content_vector_indexed_time=None):
        """Update chunk vector index status and time"""
        with self.db_session_class() as session:
            cmd = """UPDATE project_connection SET content_vector_status=:content_vector_status"""
            params = {'connection_id': connection_id, 'content_vector_status': content_vector_status}
            if last_content_vector_indexed_time:
                cmd += ", content_vector_indexed_at=:content_vector_indexed_at"
                params['content_vector_indexed_at'] = last_content_vector_indexed_time
            cmd += " WHERE id = :connection_id"
            session.execute(text(cmd), params)
            session.commit()

    def get_connections_to_recover(self, need_recover_sync_types):
        with self.db_session_class() as session:
            need_sync_types_combine_str = ','.join([f"'{t}'" for t in need_recover_sync_types])
            sql = f"""SELECT pc.id, pc.type, pc.status FROM `project_connection` pc INNER JOIN projects p
            ON p.uuid=pc.project_uuid
                     WHERE (JSON_UNQUOTE(JSON_EXTRACT(pc.`status`, '$.last_sync_status')) in ('crawling', 'pending')
                      OR JSON_UNQUOTE(JSON_EXTRACT(pc.`status`, '$.last_index_status')) = 'indexing'
                      OR JSON_UNQUOTE(JSON_EXTRACT(pc.`ai_status`, '$.last_ai_index_status')) = 'indexing'
                      OR JSON_UNQUOTE(JSON_EXTRACT(pc.`ai_status`, '$.last_ai_processing_status')) = 'processing'
                      OR JSON_UNQUOTE(JSON_EXTRACT(pc.`content_vector_status`, '$.last_content_vector_index_status')) = 'indexing'
                      )
                     AND pc.deleted=false 
                     AND p.deleted=false 
                     AND pc.is_active=true 
                     AND pc.type IN ({need_sync_types_combine_str})"""

            return session.execute(text(sql)).fetchall()

    def get_all_active_projects(self):
        with self.db_session_class() as session:
            sql = """SELECT uuid, knowledge_base_indexed_at FROM projects WHERE deleted=false"""
            return session.execute(text(sql)).fetchall()

    def update_knowledge_base_indexed_at(self, project_uuid, indexed_at):
        project_uuid = uuid_str_to_32_chars(project_uuid)
        with self.db_session_class() as session:
            cmd = """UPDATE projects SET knowledge_base_indexed_at=:indexed_at WHERE uuid=:project_uuid"""
            session.execute(text(cmd), {'project_uuid': project_uuid, 'indexed_at': indexed_at})
            session.commit()

    def update_ticket_indexed_at(self, project_uuid, indexed_at):
        project_uuid = uuid_str_to_32_chars(project_uuid)
        with self.db_session_class() as session:
            cmd = """UPDATE projects SET ticket_indexed_at=:indexed_at WHERE uuid=:project_uuid"""
            session.execute(text(cmd), {'project_uuid': project_uuid, 'indexed_at': indexed_at})
            session.commit()

    def update_portal_issue_indexed_at(self, project_uuid, indexed_at):
        project_uuid = uuid_str_to_32_chars(project_uuid)
        with self.db_session_class() as session:
            cmd = """UPDATE projects SET portal_issue_indexed_at=:indexed_at WHERE uuid=:project_uuid"""
            session.execute(text(cmd), {'project_uuid': project_uuid, 'indexed_at': indexed_at})
            session.commit()

    def update_knowledge_base_ai_indexed_at(self, project_uuid, ai_indexed_at):
        project_uuid = uuid_str_to_32_chars(project_uuid)
        with self.db_session_class() as session:
            cmd = """UPDATE projects SET knowledge_base_ai_indexed_at=:ai_indexed_at WHERE uuid=:project_uuid"""
            session.execute(text(cmd), {'project_uuid': project_uuid, 'ai_indexed_at': ai_indexed_at})
            session.commit()

    def update_ticket_ai_indexed_at(self, project_uuid, ai_indexed_at):
        project_uuid = uuid_str_to_32_chars(project_uuid)
        with self.db_session_class() as session:
            cmd = """UPDATE projects SET ticket_ai_indexed_at=:ai_indexed_at WHERE uuid=:project_uuid"""
            session.execute(text(cmd), {'project_uuid': project_uuid, 'ai_indexed_at': ai_indexed_at})
            session.commit()

    def update_portal_issue_ai_indexed_at(self, project_uuid, ai_indexed_at):
        project_uuid = uuid_str_to_32_chars(project_uuid)
        with self.db_session_class() as session:
            cmd = """UPDATE projects SET portal_issue_ai_indexed_at=:ai_indexed_at WHERE uuid=:project_uuid"""
            session.execute(text(cmd), {'project_uuid': project_uuid, 'ai_indexed_at': ai_indexed_at})
            session.commit()

    def get_project_info_by_project_uuid(self, project_uuid):
        project_uuid = uuid_str_to_32_chars(project_uuid)
        with self.db_session_class() as session:
            sql = "SELECT knowledge_base_indexed_at FROM projects WHERE uuid=:project_uuid AND deleted=false"
            return session.execute(text(sql), {'project_uuid': project_uuid}).fetchone()

    def get_ticket_indexed_at_by_project_uuid(self, project_uuid):
        project_uuid = uuid_str_to_32_chars(project_uuid)
        with self.db_session_class() as session:
            sql = "SELECT ticket_indexed_at FROM projects WHERE uuid=:project_uuid AND deleted=false"
            return session.execute(text(sql), {'project_uuid': project_uuid}).fetchone()

    def get_portal_issue_indexed_at_by_project_uuid(self, project_uuid):
        project_uuid = uuid_str_to_32_chars(project_uuid)
        with self.db_session_class() as session:
            sql = "SELECT portal_issue_indexed_at FROM projects WHERE uuid=:project_uuid AND deleted=false"
            return session.execute(text(sql), {'project_uuid': project_uuid}).fetchone()

    def get_kb_ai_info_by_project_uuid(self, project_uuid):
        project_uuid = uuid_str_to_32_chars(project_uuid)
        with self.db_session_class() as session:
            sql = """SELECT uuid, knowledge_base_ai_indexed_at FROM projects WHERE uuid=:project_uuid AND (deleted=false OR deleted IS NULL)"""
            return session.execute(text(sql), {'project_uuid': project_uuid}).fetchone()

    def get_ticket_ai_info_by_project_uuid(self, project_uuid):
        project_uuid = uuid_str_to_32_chars(project_uuid)
        with self.db_session_class() as session:
            sql = "SELECT ticket_ai_indexed_at FROM projects WHERE uuid=:project_uuid AND deleted=false"
            return session.execute(text(sql), {'project_uuid': project_uuid}).fetchone()

    def get_portal_issue_ai_info_by_project_uuid(self, project_uuid):
        project_uuid = uuid_str_to_32_chars(project_uuid)
        with self.db_session_class() as session:
            sql = "SELECT portal_issue_ai_indexed_at FROM projects WHERE uuid=:project_uuid AND deleted=false"
            return session.execute(text(sql), {'project_uuid': project_uuid}).fetchone()

    def get_recently_updated_connections(self, days=7):
        with self.db_session_class() as session:
            sql = """SELECT pc.id, pc.type, pc.project_uuid, pc.ai_indexed_at, pc.content_vector_indexed_at
                     FROM project_connection pc
                     INNER JOIN projects p ON p.uuid = pc.project_uuid
                     WHERE p.deleted = false 
                     AND pc.deleted = false
                     AND pc.last_sync_time >= DATE_SUB(NOW(), INTERVAL :days DAY)"""
            return session.execute(text(sql), {'days': days}).fetchall()

    def get_project_confluence_oauth(self, project_uuid):
        project_uuid = uuid_str_to_32_chars(project_uuid)
        with self.db_session_class() as session:
            sql = """
                SELECT project_uuid, access_token, refresh_token, expires_at
                FROM project_confluence_oauth
                WHERE project_uuid = :project_uuid
            """
            return session.execute(text(sql), {'project_uuid': project_uuid}).fetchone()

    def get_confluence_oauth_page(self, offset=0, limit=100):
        """Paginated query of project_confluence_oauth."""
        with self.db_session_class() as session:
            sql = """
                SELECT project_uuid, access_token, refresh_token, expires_at
                FROM project_confluence_oauth
                ORDER BY project_uuid
                LIMIT :limit OFFSET :offset
            """
            return session.execute(
                text(sql), {'limit': limit, 'offset': offset}
            ).fetchall()

    def update_project_confluence_oauth(self, project_uuid, access_token, expires_at, refresh_token):
        project_uuid = uuid_str_to_32_chars(project_uuid)
        with self.db_session_class() as session:
            exists_sql = """
                SELECT 1
                FROM project_confluence_oauth
                WHERE project_uuid = :project_uuid
            """
            exists = session.execute(text(exists_sql), {'project_uuid': project_uuid}).fetchone()

            if exists:
                sql = """
                    UPDATE project_confluence_oauth
                    SET access_token = :access_token,
                        refresh_token = :refresh_token,
                        expires_at = :expires_at
                    WHERE project_uuid = :project_uuid
                """
            else:
                sql = """
                    INSERT INTO project_confluence_oauth
                    (project_uuid, access_token, refresh_token, expires_at)
                    VALUES (:project_uuid, :access_token, :refresh_token, :expires_at)
                """

            session.execute(text(sql), {
                'project_uuid': project_uuid,
                'access_token': access_token,
                'refresh_token': refresh_token,
                'expires_at': expires_at,
            })
            session.commit()

    def get_recently_updated_projects(self, days=7):
        with self.db_session_class() as session:
            sql = """SELECT uuid
                     FROM projects 
                     WHERE deleted = false 
                     AND (knowledge_base_indexed_at >= DATE_SUB(NOW(), INTERVAL :days DAY) 
                     OR ticket_indexed_at >= DATE_SUB(NOW(), INTERVAL :days DAY) 
                     OR portal_issue_indexed_at >= DATE_SUB(NOW(), INTERVAL :days DAY))
                     """
            return session.execute(text(sql), {'days': days}).fetchall()

    def get_need_cleaned_connections_by_type(self, recent_days, connection_type):
        with self.db_session_class() as session:
            sql = """
                SELECT pc.id, pc.config, pc.project_uuid, pc.last_cleaned_at
                FROM project_connection pc
                INNER JOIN projects p ON p.uuid = pc.project_uuid
                WHERE p.deleted = false
                AND pc.deleted = false
                AND pc.is_active = true
                AND pc.type = :connection_type
                AND (
                    pc.last_cleaned_at IS NULL
                    OR pc.last_cleaned_at <= DATE_SUB(NOW(), INTERVAL :recent_days DAY)
                )
            """
            return session.execute(text(sql), {'recent_days': recent_days, 'connection_type': connection_type}).fetchall()

    def get_github_connections_need_options_sync(self, recent_days):
        with self.db_session_class() as session:
            sql = """
                SELECT pc.id, pc.config, pc.project_uuid, pc.last_options_synced_at
                FROM project_connection pc
                INNER JOIN projects p ON p.uuid = pc.project_uuid
                WHERE p.deleted = false
                AND pc.deleted = false
                AND pc.is_active = true
                AND pc.type = :connection_type
                AND (
                    pc.last_options_synced_at IS NULL
                    OR pc.last_options_synced_at <= DATE_SUB(NOW(), INTERVAL :recent_days DAY)
                )
            """
            return session.execute(
                text(sql),
                {'recent_days': recent_days, 'connection_type': ConnectionType.GITHUB_ISSUE.value}
            ).fetchall()

    def update_connection_last_options_synced_at(self, connection_id, synced_at):
        with self.db_session_class() as session:
            sql = """
                UPDATE project_connection
                SET last_options_synced_at = :synced_at
                WHERE id = :connection_id
            """
            session.execute(text(sql), {'synced_at': synced_at, 'connection_id': connection_id})
            session.commit()

    def update_connection_last_cleaned_at(self, connection_id, cleaned_at):
        with self.db_session_class() as session:
            sql = """
                UPDATE project_connection
                SET last_cleaned_at = :cleaned_at
                WHERE id = :connection_id
            """
            session.execute(text(sql), {'cleaned_at': cleaned_at, 'connection_id': connection_id})
            session.commit()

    def uninstall_github_app(self, project_uuid, installation_id):
        project_uuid = uuid_str_to_36_chars(project_uuid)
        with self.db_session_class() as session:
            sql = """
                DELETE FROM project_github_app_installation
                WHERE project_uuid=:project_uuid and installation_id=:installation_id       
            """
            session.execute(text(sql), {'project_uuid': project_uuid, 'installation_id': installation_id})
            session.commit()
