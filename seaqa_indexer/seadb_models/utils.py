import logging

from seaqa_indexer.seadb_models.schema_loader import get_seadb_table_schemas

logger = logging.getLogger(__name__)


class TableNameInvalidError(Exception):
    pass


class ColumnNameInvalidError(Exception):
    pass

def _resolve_table_name_by_table_name_schema(table_name_schema, connection_id=None):
    if '{connection_id}' in table_name_schema:
        if connection_id is None:
            raise ValueError(f'connection_id is required for table name: {table_name_schema}')
        return table_name_schema.format(connection_id=connection_id)
    return table_name_schema


def get_table_name_from_schema(schema_table_name, connection_id=None):
    """Resolve the actual seadb table name from the YAML schema for a given schema table name."""
    schema = get_seadb_table_schemas()
    tables = schema.get('tables') or {}
    table_schema = tables.get(schema_table_name) or {}
    table_name_schema = table_schema.get('table_name')
    table_name = _resolve_table_name_by_table_name_schema(table_name_schema, connection_id)
    if not table_name:
        raise TableNameInvalidError()
    return table_name


def get_column_name_from_schema(schema_table_name, column_name):
    """Return the column_name defined in YAML schema, or empty string if column not found. O(1) dict lookup."""
    schema = get_seadb_table_schemas()
    tables = schema.get('tables') or {}
    table_schema = tables.get(schema_table_name) or {}
    columns = table_schema.get('columns') or {}
    if not column_name in columns:
        raise ColumnNameInvalidError()
    return column_name


def get_column_data_from_schema(schema_table_name, column_name):
    """Return the column_data config for the specified column from YAML schema. O(1) dict lookup."""
    schema = get_seadb_table_schemas()
    tables = schema.get('tables') or {}
    table_schema = tables.get(schema_table_name) or {}
    columns = table_schema.get('columns') or {}
    col = columns.get(column_name)
    if col:
        return col.get('column_data') or {}
    return {}
