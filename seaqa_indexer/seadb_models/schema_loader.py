import logging

import yaml

from seaqa_indexer.config import SEADB_TABLE_SCHEMA_PATH

logger = logging.getLogger(__name__)
_SCHEMA_CACHE = None


def get_seadb_table_schemas():
    """Load and cache the seadb table schemas from YAML."""
    global _SCHEMA_CACHE
    if _SCHEMA_CACHE is not None:
        return _SCHEMA_CACHE

    yaml_path = SEADB_TABLE_SCHEMA_PATH
    logger.info(f'Loading seadb table schemas from: {yaml_path}')
    with open(yaml_path, 'r', encoding='utf-8') as f:
        _SCHEMA_CACHE = yaml.safe_load(f)
    return _SCHEMA_CACHE
