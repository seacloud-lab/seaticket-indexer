from enum import Enum


class CrawlStatus:
    PENDING = 'pending'
    CRAWLING = 'crawling'
    COMPLETED = 'completed'
    FAILED = 'failed'


class IndexStatus:
    PENDING = 'pending'
    INDEXING = 'indexing'
    COMPLETED = 'completed'
    FAILED = 'failed'


class AIStatus:
    PENDING = 'pending'
    PROCESSING = 'processing'
    COMPLETED = 'completed'
    FAILED = 'failed'

class ConnectionType(Enum):
    EMAIL = 'email'
    GITHUB_ISSUE = 'github_issue'
    DISCOURSE_FORUM = 'discourse_forum'
    SITE = 'site'
    SEAFILE = 'seafile'


CONNECTION_TYPE_TO_INDEX_PREFIX = {
    connection_type.value: connection_type.value + '_' for connection_type in ConnectionType
}

text_suffixes = [
    'txt',
    'md',
    'markdown'
]

office_suffixes = [
    'docx',
    'pptx',
    'pdf',
    'odt',
    'ods',
    'odp',
    'sdoc',
]

REPO_FILE_SIZE_LIMIT = 25 * 1024 * 1024

REPO_NAME_CACHE_TIME_OUT = 24 * 60 * 60

CONNECTION_STATUS_UPDATE_LIMIT = 100

CONNECTION_STATUS_MIN_UPDATE_COUNT = 20
CONNECTION_STATUS_UPDATE_INTERVAL_SECONDS = 5
LLM_INPUT_CHARACTERS_LIMIT = 4000

SEADB_CONTENT_SIZE_LIMIT = 100 * 1000

SYNC_FILE_SUFFIXES = office_suffixes + text_suffixes

INDEX_CONTENT_LIMIT = 100 * 1000
