import logging
from copy import deepcopy

from seaqa_indexer.seadb_models.schema_loader import get_seadb_table_schemas

logger = logging.getLogger(__name__)


def _resolve_table_name(table_name_schema, connection_id=None):
    if '{connection_id}' in table_name_schema:
        if connection_id is None:
            raise ValueError(f'connection_id is required for table name: {table_name_schema}')
        return table_name_schema.format(connection_id=connection_id)
    return table_name_schema


def _normalize_index(index_item):
    if isinstance(index_item, str):
        return [index_item]
    if isinstance(index_item, list):
        return index_item
    raise ValueError(f'Invalid index config: {index_item}')


def get_table_name(schema_table_name, connection_id=None):
    """Resolve the actual seadb table name from the YAML schema for a given schema table name."""
    schema = get_seadb_table_schemas()
    tables = schema.get('tables') or {}
    table_schema = tables.get(schema_table_name) or {}
    table_name_schema = table_schema.get('table_name')
    if table_name_schema:
        return _resolve_table_name(table_name_schema, connection_id)
    return ''


def get_column_name(schema_table_name, column_name):
    """Return the column_name defined in YAML schema, or empty string if column not found. O(1) dict lookup."""
    schema = get_seadb_table_schemas()
    tables = schema.get('tables') or {}
    table_schema = tables.get(schema_table_name) or {}
    columns = table_schema.get('columns') or {}
    if column_name in columns:
        return column_name
    return ''


def get_column_data(schema_table_name, column_name):
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


def init_seadb_tables_from_schema(schema_key, seadb_api, project_uuid, connection_id=None):
    """Create seadb tables and columns from the YAML schema definition."""
    schema = get_seadb_table_schemas()
    table_defs = schema.get('tables') or {}
    init_groups = schema.get('init_groups') or {}
    schema_table_names = init_groups.get(schema_key, [])
    for schema_table_name in schema_table_names:
        table_schema = table_defs.get(schema_table_name) or {}
        table_name = _resolve_table_name(table_schema.get('table_name', ''), connection_id)
        res = seadb_api.create_table(project_uuid, table_name)
        table_id = res['table_id']

        cascade = table_schema.get('cascade') or {}
        source_column = cascade.get('source_column')
        target_column = cascade.get('target_column')
        source_column_key = None

        for column_name, column in table_schema.get('columns', {}).items():
            mapped_column = deepcopy(column)
            mapped_column['column_name'] = column_name
            if column_name == target_column and source_column_key:
                mapped_column.setdefault('column_data', {})
                mapped_column['column_data']['cascade_column_key'] = source_column_key

            added_column = seadb_api.add_column(project_uuid, table_id, mapped_column)
            if column_name == source_column:
                source_column_key = added_column['column_key']

        for index_item in table_schema.get('indexes', []):
            seadb_api.create_column_index(project_uuid, table_id, _normalize_index(index_item))