import logging
import os
from dataclasses import dataclass, field
import threading

import yaml

logger = logging.getLogger(__name__)

_SCHEMA_LOCK = threading.Lock()
_SCHEMA_CACHE = None


@dataclass
class ColumnSchema:
    name: str
    type: str | None = None
    data: dict = field(default_factory=dict)

    def get(self, key, default=None):
        if key == 'name':
            return self.name
        if key == 'column_type':
            return self.type
        if key == 'column_data':
            return self.data
        return default

    def to_dict(self):
        data = {
            'column_name': self.name,
            'column_type': self.type,
        }
        if self.data:
            data['column_data'] = self.data
        return data


class ColumnNamespace:
    def __init__(self, columns_dict):
        self._columns = columns_dict

    def __getattr__(self, column_name):
        try:
            return self._columns[column_name]
        except KeyError as exc:
            raise AttributeError(f"Column '{column_name}' not found") from exc

    def __iter__(self):
        return iter(self._columns.values())

    def values(self):
        return self._columns.values()

    def __contains__(self, item):
        return item in self._columns

    def get(self, key, default=None):
        return self._columns.get(key, default)

    def keys(self):
        return self._columns.keys()

    def items(self):
        return self._columns.items()

@dataclass
class TableSchema:
    name: str
    table_name_schema: str | None = None
    _columns_dict: dict[str, ColumnSchema] = field(default_factory=dict)
    indexes: list = field(default_factory=list)

    @property
    def column(self) -> ColumnNamespace:
        return ColumnNamespace(self._columns_dict)

    def get(self, key, default=None):
        if key == 'name':
            return self.name
        if key == 'table_name':
            return self.table_name_schema
        if key == 'columns':
            return self._columns_dict
        if key == 'indexes':
            return self.indexes
        return default

    def table_name(self, connection_id=None):
        if not self.table_name_schema:
            return ''
        if '{connection_id}' in self.table_name_schema:
            if connection_id is None:
                raise ValueError(f'connection_id is required for table name: {self.table_name_schema}')
            return self.table_name_schema.format(connection_id=connection_id)
        return self.table_name_schema

    def get_fields(self):
        return list(self._columns_dict.values())


@dataclass
class SchemaRegistry:
    tables: dict[str, TableSchema] = field(default_factory=dict)

    def __getattr__(self, table_name):
        try:
            return self.tables[table_name]
        except KeyError as exc:
            raise AttributeError(table_name) from exc

    def get(self, key, default=None):
        if key == 'tables':
            return self.tables
        return default


def _build_schema_registry(raw_schema):
    tables = {}
    for table_name, table_schema in (raw_schema.get('tables') or {}).items():
        columns = {}
        for column_name, column_schema in (table_schema.get('columns') or {}).items():
            columns[column_name] = ColumnSchema(
                name=column_name,
                type=column_schema.get('column_type'),
                data=column_schema.get('column_data') or {},
            )
        tables[table_name] = TableSchema(
            name=table_name,
            table_name_schema=table_schema.get('table_name'),
            _columns_dict=columns,
            indexes=table_schema.get('indexes') or [],
        )
    return SchemaRegistry(tables=tables)


def _schema_file_path():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), 'table_schemas.yaml')


def preload_seadb_table_schemas():
    global _SCHEMA_CACHE
    if _SCHEMA_CACHE is not None:
        return _SCHEMA_CACHE

    with _SCHEMA_LOCK:
        if _SCHEMA_CACHE is not None:
            return _SCHEMA_CACHE
        yaml_path = _schema_file_path()
        if not os.path.isfile(yaml_path):
            raise FileNotFoundError(f'Seadb table schema file not found: {yaml_path}')
        logger.info(f'Loading seadb table schemas from: {yaml_path}')
        with open(yaml_path, 'r', encoding='utf-8') as fp:
            _SCHEMA_CACHE = _build_schema_registry(yaml.safe_load(fp) or {})
    return _SCHEMA_CACHE


def get_seadb_table_schemas():
    return preload_seadb_table_schemas()


SCHEMA = get_seadb_table_schemas()
