import logging

logger = logging.getLogger(__name__)


class TableNameInvalidError(Exception):
    pass


class ColumnNameInvalidError(Exception):
    pass

