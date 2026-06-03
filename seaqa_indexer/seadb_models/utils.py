import logging

from seaqa_indexer.seadb_models.schema_loader import get_seadb_table_schemas

logger = logging.getLogger(__name__)


def _resolve_table_name_by_schema_table_key(schema_table_name, connection_id=None):
    if '{connection_id}' in schema_table_name:
        if connection_id is None:
            raise ValueError(f'connection_id is required for table name: {schema_table_name}')
        return schema_table_name.format(connection_id=connection_id)
    return schema_table_name


def get_table_name_from_schema(schema_table_name, connection_id=None):
    """Resolve the actual seadb table name from the YAML schema for a given schema table name."""
    schema = get_seadb_table_schemas()
    tables = schema.get('tables') or {}
    table_schema = tables.get(schema_table_name) or {}
    table_name_schema = table_schema.get('table_name')
    if table_name_schema:
        return _resolve_table_name_by_schema_table_key(table_name_schema, connection_id)
    return ''


def get_column_name_from_schema(schema_table_name, column_name):
    """Return the column_name defined in YAML schema, or empty string if column not found. O(1) dict lookup."""
    schema = get_seadb_table_schemas()
    tables = schema.get('tables') or {}
    table_schema = tables.get(schema_table_name) or {}
    columns = table_schema.get('columns') or {}
    if column_name in columns:
        return column_name
    return ''


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


def get_columns(schema_table_name):
    """Return all column definitions for a given schema table name from YAML."""
    schema = get_seadb_table_schemas()
    tables = schema.get('tables') or {}
    table_schema = tables.get(schema_table_name) or {}
    return table_schema.get('columns') or {}
