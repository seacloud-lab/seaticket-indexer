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

REPO_FILE_SIZE_LIMIT = 10 * 1024 * 1024

REPO_NAME_CACHE_TIME_OUT = 24 * 60 * 60

CONNECTION_STATUS_UPDATE_LIMIT = 100


# web crawl table
class PropertyTypes:
    TEXT = 'text'
    DATETIME = 'datetime'
    INT = 'int64'
    FLOAT = 'float64'
    SINGLE_SELECT = 'single-select'
    MULTIPLE_SELECT = 'multiple-select'
    BOOL = 'bool'


class WebCrawlTable(object):

    @property
    def columns(self):
        return WebCrawlColumns()


class WebCrawlColumns(object):
    def __init__(self):
        self.url = WebCrawlColumn('url', PropertyTypes.TEXT)
        self.title = WebCrawlColumn('title', PropertyTypes.TEXT)
        self.etag = WebCrawlColumn('etag', PropertyTypes.TEXT)
        self.last_modified = WebCrawlColumn('last_modified', PropertyTypes.DATETIME)
        self.updated_at = WebCrawlColumn('updated_at', PropertyTypes.DATETIME)
        self.deleted = WebCrawlColumn('deleted', PropertyTypes.BOOL)

class WebCrawlColumn(object):
    def __init__(self, name, type, data=None):
        self.name = name
        self.type = type
        self.data = data

    def to_dict(self, data=None):
        column_data = {
            'name': self.name,
            'type': self.type,
        }
        if self.data:
            column_data['data'] = self.data

        if data:
            column_data['data'] = data

        return column_data

WEB_CRAWL_TABLE = WebCrawlTable()
WEB_CRAWL_COLUMNS = [
    WEB_CRAWL_TABLE.columns.url.to_dict(),
    WEB_CRAWL_TABLE.columns.title.to_dict(),
    WEB_CRAWL_TABLE.columns.etag.to_dict(),
    WEB_CRAWL_TABLE.columns.last_modified.to_dict(),
    WEB_CRAWL_TABLE.columns.updated_at.to_dict(),
    WEB_CRAWL_TABLE.columns.deleted.to_dict(),
]
