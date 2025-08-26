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


# crawl output table

class PropertyTypes:
    TEXT = 'text'
    DATE = 'date'
    LONG_TEXT = 'long-text'
    CHECKBOX = 'checkbox'
    SINGLE_SELECT = 'single-select'
    MULTIPLE_SELECT = 'multiple-select'
    URL = 'url'
    DURATION = 'duration'
    NUMBER = 'number'
    COLLABORATOR = 'collaborator'
    EMAIL = 'email'
    CTIME = 'ctime'
    MTIME = 'mtime'
 


class CrawlOutputTable(object):
    def __init__(self, table_id, name):
        self.id = table_id
        self.name = name

    @property
    def columns(self):
        return CrawlOutputColumns()



class PrivatePropertyKeys:
    URL = '_url'
    TITLE = '_title'
    # CONTENT = '_content'
    ETAG = '_etag'
    LAST_MODIFIED = '_last_modified'

class CrawlOutputColumns(object):
    def __init__(self):
        self.url = CrawlOutputColumn(PrivatePropertyKeys.URL, '_url', PropertyTypes.TEXT)
        self.title = CrawlOutputColumn(PrivatePropertyKeys.TITLE, '_title', PropertyTypes.TEXT)
        # self.content = CrawlOutputColumn(PrivatePropertyKeys.CONTENT, '_content', PropertyTypes.TEXT)
        self.etag = CrawlOutputColumn(PrivatePropertyKeys.ETAG, '_etag', PropertyTypes.TEXT)
        self.last_modified = CrawlOutputColumn(PrivatePropertyKeys.LAST_MODIFIED, '_last_modified', PropertyTypes.TEXT)

class CrawlOutputColumn(object):
    def __init__(self, key, name, type, data=None):
        self.key = key
        self.name = name
        self.type = type
        self.data = data

    def to_dict(self, data=None):
        column_data = {
            'key': self.key,
            'name': self.name,
            'type': self.type,
        }
        if self.data:
            column_data['data'] = self.data

        if data:
            column_data['data'] = data

        return column_data

CRAWL_OUTPUT_TABLE = CrawlOutputTable('0000', 'Table1')
CRAWL_OUTPUT_COLUMNS = [
    CRAWL_OUTPUT_TABLE.columns.url.to_dict(),
    CRAWL_OUTPUT_TABLE.columns.title.to_dict(),
    # CRAWL_OUTPUT_TABLE.columns.content.to_dict(),
    CRAWL_OUTPUT_TABLE.columns.etag.to_dict(),
    CRAWL_OUTPUT_TABLE.columns.last_modified.to_dict(),
]