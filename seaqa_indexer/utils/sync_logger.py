import datetime
import logging
from sqlalchemy import text
from seaqa_indexer.config import LOG_LEVEL

logger = logging.getLogger(__name__)

LEVEL_MAP = {
    'CRITICAL': logging.CRITICAL,
    'ERROR': logging.ERROR,
    'WARNING': logging.WARNING,
    'INFO': logging.INFO,
    'DEBUG': logging.DEBUG,
}

class SyncLogger:
    def __init__(self, db_session_class):
        self.db_session_class = db_session_class
        self.log_lines = []
        self._attached_handlers = []
        self._capture_started = False
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
            logger.error(f"Failed to initialize sync log header: {e}")
        self.start_capture()
        return self
    
    def _threshold_level(self):
        return LEVEL_MAP.get(LOG_LEVEL.upper(), logging.INFO)

    def _parse_level_from_message(self, message):
        try:
            if isinstance(message, str) and message.startswith('['):
                end = message.find(']')
                if end > 1:
                    lvl_name = message[1:end].strip().upper()
                    return LEVEL_MAP.get(lvl_name, logging.INFO)
        except Exception:
            pass
        return logging.INFO

    def _add_log_line(self, message):
        msg_level = self._parse_level_from_message(message)
        if msg_level < self._threshold_level():
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
    
    def info(self, message, extra=None):
        if extra:
            line = extra.get('line')
            file_name = extra.get('file_name')
            message = f"[INFO] {file_name}:{line} {message}"
        self._add_log_line(message)
    
    def warning(self, message, extra=None):
        if extra:
            line = extra.get('line')
            file_name = extra.get('file_name')
            message = f"[WARNING] {file_name}:{line} {message}"
        self._add_log_line(message)
    
    def error(self, message, extra=None):
        if extra:
            line = extra.get('line')
            file_name = extra.get('file_name')
            message = f"[ERROR] {file_name}:{line} {message}"
        self._add_log_line(message)
    
    def debug(self, message, extra=None):
        if extra:
            line = extra.get('line')
            file_name = extra.get('file_name')
            message = f"[DEBUG] {file_name}:{line} {message}"
        self._add_log_line(message)
        
    def flush(self):
        self._flush_log_lines()
        
    class _SyncLogHandler(logging.Handler):
        def __init__(self, sync_logger_ref):
            super().__init__()
            self._sync_logger = sync_logger_ref

        def emit(self, record: logging.LogRecord) -> None:
            try:
                msg = self.format(record) if self.formatter else record.getMessage()
                self._sync_logger._add_log_line(msg)
            except Exception:
                pass

    def start_capture(self, logger_names=None, level=None):
        if self._capture_started:
            return
        if logger_names is None:
            logger_names = [
                'trafilatura',
                'scrapy',
            ]
        handler = SyncLogger._SyncLogHandler(self)
        effective_level = level if level is not None else LEVEL_MAP.get(str(LOG_LEVEL).upper(), logging.INFO)
        handler.setLevel(effective_level)
        formatter = logging.Formatter('[%(levelname)s] %(name)s:%(lineno)d %(message)s')
        handler.setFormatter(formatter)

        for name in logger_names:
            lg = logging.getLogger(name)
            lg.addHandler(handler)
            self._attached_handlers.append((lg, handler))
        self._capture_started = True

    def stop_capture(self):
        for lg, handler in self._attached_handlers:
            try:
                lg.removeHandler(handler)
                handler.close()
            except Exception:
                pass
        self._attached_handlers = []
        self._capture_started = False
        self._flush_log_lines()
        