import datetime
import logging
from sqlalchemy import text

logger = logging.getLogger(__name__)


class SyncLogger:
    def __init__(self, db_session_class):
        self.db_session_class = db_session_class
        self.log_lines = []
        self.connection_id = None
    
    def init(self, connection_id):
        self.connection_id = connection_id
        try:
            with self.db_session_class() as session:
                update_sql = """
                    UPDATE project_connection
                    SET log_details = :details
                    WHERE id = :connection_id
                """
                session.execute(text(update_sql), {"details": '', "connection_id": self.connection_id})
                session.commit()
        except Exception as e:
            logger.error(f"Failed to initialize sync log: {e}")
        return self
    
    def _add_log_line(self, message):
        if not message:
            return
        line = f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}"
        self.log_lines.append(line)
        if len(self.log_lines) >= 100:
            self._flush_log_lines()
    
    def _flush_log_lines(self):
        if not self.log_lines:
            return
        new_details = "\n".join(self.log_lines)
        append_text = "\n" + new_details if new_details else ""
        try:
            with self.db_session_class() as session:
                update_sql = """
                    UPDATE project_connection
                    SET log_details = CONCAT(COALESCE(log_details, ''), :append_text)
                    WHERE id = :connection_id
                """
                session.execute(text(update_sql), {"append_text": append_text, "connection_id": self.connection_id})
                session.commit()
            self.log_lines = []
        except Exception as e:
            logger.error(f"Failed to flush log lines: {e}")
    
    def log(self, message):
        self._add_log_line(message)

    def stop_capture(self):
        self._flush_log_lines()
