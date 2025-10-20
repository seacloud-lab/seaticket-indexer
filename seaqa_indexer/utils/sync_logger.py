import datetime
import logging
import uuid
from sqlalchemy import text


class SyncLogger:
    def __init__(self, db_session_class):
        self.db_session_class = db_session_class
        self.log_lines = []
        self._attached_handlers = []
    
    def init(self, connection_id):
        self.connection_id = connection_id
        self.sync_id = uuid.uuid4().hex
        start_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.header_line = f"=== Sync Start [{self.sync_id}] connection={self.connection_id} at {start_time} ==="
        self.log_lines = [self.header_line]
        self.start_capture()
        return self
    
    def _add_log_line(self, message):
        line = f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {message}"
        self.log_lines.append(line)
        if len(self.log_lines) >= 100:
            self._flush_log_lines()
    
    def _flush_log_lines(self):
        if not self.log_lines:
            return
        new_details = "\n".join(self.log_lines)
        with self.db_session_class() as session:
            select_sql = """
                SELECT log_details FROM project_connection WHERE id = :connection_id
            """
            row = session.execute(text(select_sql), {"connection_id": self.connection_id}).fetchone()
            if not row:
                self.log_lines = []
                return
            existing_details = row.log_details if hasattr(row, 'log_details') else row[1]

            if isinstance(existing_details, str) and existing_details.startswith(self.header_line):
                content_to_write = existing_details
                if new_details:
                    content_to_write = existing_details + "\n" + new_details
            else:
                content_to_write = new_details if new_details.startswith(self.header_line) else (self.header_line + ("\n" + new_details if new_details else ""))

            update_sql = """
                UPDATE project_connection
                SET log_details = :details
                WHERE id = :connection_id
            """
            session.execute(text(update_sql), {"details": content_to_write, "connection_id": self.connection_id})
            session.commit()
        self.log_lines = []
    
    def _log_info(self, message):
        self._add_log_line(f"[INFO] {message}")
    
    def _log_warning(self, message):
        self._add_log_line(f"[WARNING] {message}")
    
    def _log_error(self, message):
        self._add_log_line(f"[ERROR] {message}")
    
    def _log_debug(self, message):
        self._add_log_line(f"[DEBUG] {message}")
        
    def update_connection_log_details(self, details):
        pass

    class _SyncLogHandler(logging.Handler):
        def __init__(self, sync_logger_ref):
            super().__init__()
            self._sync_logger = sync_logger_ref

        def emit(self, record: logging.LogRecord) -> None:
            try:
                level = record.levelname
                logger_name = record.name
                msg = self.format(record) if self.formatter else record.getMessage()
                self._sync_logger._add_log_line(f"[{level}] {logger_name}: {msg}")
            except Exception:
                pass

    def start_capture(self, logger_names=None, level=logging.INFO):
        if logger_names is None:
            logger_names = [
                'trafilatura',
                'scrapy',
            ]
        handler = SyncLogger._SyncLogHandler(self)
        handler.setLevel(level)
        formatter = logging.Formatter('%(message)s')
        handler.setFormatter(formatter)

        for name in logger_names:
            lg = logging.getLogger(name)
            lg.addHandler(handler)
            self._attached_handlers.append((lg, handler))

    def stop_capture(self):
        for lg, handler in self._attached_handlers:
            try:
                lg.removeHandler(handler)
            except Exception:
                pass
        self._attached_handlers = []
        