import logging
from sqlalchemy import text


logger = logging.getLogger('__name__')


class SeaqaDB(object):
    def __init__(self, db_session):
        self.db_session = db_session

    def get_project_connection_by_id(self, connection_id):
        with self.db_session() as session:
            sql = """SELECT pc.id, pc.config, pc.project_id, pc.status, p.uuid as project_uuid, pc.updated_at, pc.type
            FROM project_connection pc 
            INNER JOIN projects p 
            ON p.id=pc.project_id WHERE p.deleted=false AND pc.id=:connection_id"""
            connection = session.execute(text(sql), {'connection_id': connection_id}).fetchone()
            return connection

    def update_connection_status(self, connection_id, status):
        with self.db_session() as session:
            cmd = "UPDATE project_connection SET status = :status WHERE id = :connection_id"
            session.execute(text(cmd), {'connection_id': connection_id, 'status': status})
            session.commit()

    def update_connection_updated_at(self, connection_id, updated_at):
        """Update connection's updated_at timestamp"""
        with self.db_session() as session:
            sql = """UPDATE project_connection
                       SET updated_at = :updated_at
                       WHERE id = :connection_id"""
            session.execute(text(sql), {
                'updated_at': updated_at,
                'connection_id': connection_id
            })
            session.commit()

    def update_status_and_time(self, connection_id, status, updated_at):
        with self.db_session() as session:
            cmd = """UPDATE project_connection SET updated_at=:updated_at, status=:status WHERE id = :connection_id"""
            params = {'connection_id': connection_id, 'updated_at': updated_at, 'status': status}
            session.execute(text(cmd), params)
            session.commit()









    def reset_download_rate_limit(self):
        self.session.execute(text("TRUNCATE TABLE UserDownloadRateLimit"))
        self.session.execute(text("TRUNCATE TABLE OrgDownloadRateLimit"))

    def get_repo_owner(self, repo_id):
        sql = "SELECT owner_id FROM RepoOwner WHERE repo_id = :repo_id"
        result = self.session.execute(text(sql), {'repo_id': repo_id})
        rows = result.fetchone()
        if not rows:
            return None
        return rows[0]

    def get_org_repo_owner(self, repo_id):
        sql = "SELECT user FROM OrgRepo WHERE repo_id = :repo_id"
        result = self.session.execute(text(sql), {'repo_id': repo_id})
        rows = result.fetchone()
        if not rows:
            return None
        return rows[0]

    def get_user_self_usage(self, email):
        sql = """
                SELECT SUM(size)
                FROM RepoOwner o
                LEFT JOIN VirtualRepo v ON o.repo_id = v.repo_id
                JOIN RepoSize rs ON o.repo_id = rs.repo_id
                WHERE owner_id = :email
                AND v.repo_id IS NULL
                """
        result = self.session.execute(text(sql), {'email': email})
        rows = result.fetchone()
        if not rows:
            return None
        return rows[0]

    def get_org_user_quota_usage(self, org_id, email):
        sql = """
                SELECT SUM(size)
                FROM OrgRepo o
                LEFT JOIN VirtualRepo v ON o.repo_id = v.repo_id
                JOIN RepoSize rs ON o.repo_id = rs.repo_id
                WHERE org_id = :org_id
                AND user = :email
                AND v.repo_id IS NULL
                """
        result = self.session.execute(text(sql), {'org_id': org_id, 'email': email})
        rows = result.fetchone()
        if not rows:
            return None

        return rows[0]

    def get_org_id_by_repo_id(self, repo_id):
        sql = "SELECT org_id FROM OrgRepo WHERE repo_id = :repo_id"
        result = self.session.execute(text(sql), {'repo_id': repo_id})
        rows = result.fetchone()
        if not rows:
            return -1
        return rows[0]

    def get_org_quota_usage(self, org_id):
        sql = """
                SELECT SUM(size)
                FROM OrgRepo o
                LEFT JOIN VirtualRepo v ON o.repo_id = v.repo_id
                JOIN RepoSize rs ON o.repo_id = rs.repo_id
                WHERE org_id = :org_id
                AND v.repo_id IS NULL
                """
        result = self.session.execute(text(sql), {'org_id': org_id})
        rows = result.fetchone()
        if not rows:
            return None
        return rows[0]

    def get_user_quota(self, email):
        '''
        Geting user / org_user / org quota is related not only to the records in the database，
        but also to the configurations in seafile.conf.

        To simplify the logic here, the seafile_api is used to directly obtain the quota
        instead of directly searching in the database.

        '''

        return seafile_api.get_user_quota(email)

    def get_org_user_quota(self, org_id, email):

        return seafile_api.get_org_user_quota(org_id, email)

    def get_org_quota(self, org_id):

        return seafile_api.get_org_quota(org_id)
