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
    def __init__(self, table_id, name):
        self.id = table_id
        self.name = name

    @property
    def columns(self):
        return WebCrawlColumns()


class WebCrawlColumns(object):
    def __init__(self):
        self.url = WebCrawlColumn('url', PropertyTypes.TEXT)
        self.title = WebCrawlColumn('title', PropertyTypes.TEXT)
        self.etag = WebCrawlColumn('etag', PropertyTypes.TEXT)
        self.last_modified = WebCrawlColumn('last_modified', PropertyTypes.DATETIME)


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

WEB_CRAWL_TABLE = WebCrawlTable('0000', 'Table1')
WEB_CRAWL_COLUMNS = [
    WEB_CRAWL_TABLE.columns.url.to_dict(),
    WEB_CRAWL_TABLE.columns.title.to_dict(),
    WEB_CRAWL_TABLE.columns.etag.to_dict(),
    WEB_CRAWL_TABLE.columns.last_modified.to_dict(),
]


class DiscourseTopicsTable(object):
    def __init__(self, table_id, name):
        self.id = table_id
        self.name = name

    @property
    def columns(self):
        return DiscourseTopicsColumns()


class DiscourseTopicsColumns(object):
    def __init__(self):
        self.topic_id = DiscourseColumn('topic_id', PropertyTypes.INT)
        self.title = DiscourseColumn('title', PropertyTypes.TEXT)
        self.slug = DiscourseColumn('slug', PropertyTypes.TEXT)
        self.views = DiscourseColumn('views', PropertyTypes.INT)
        self.category_id = DiscourseColumn('category_id', PropertyTypes.INT)
        self.connection_id = DiscourseColumn('connection_id', PropertyTypes.INT)
        self.bumped_at = DiscourseColumn('bumped_at', PropertyTypes.DATETIME)
        self.need_index = DiscourseColumn('need_index', PropertyTypes.BOOL)
        self.deleted = DiscourseColumn('deleted', PropertyTypes.BOOL)


class DiscourseRepliesTable(object):
    def __init__(self, table_id, name):
        self.id = table_id
        self.name = name

    @property
    def columns(self):
        return DiscourseRepliesColumns()


class DiscourseRepliesColumns(object):
    def __init__(self):
        self.topic_id = DiscourseColumn('topic_id', PropertyTypes.INT)
        self.post_number = DiscourseColumn('post_number', PropertyTypes.INT)
        self.content = DiscourseColumn('content', PropertyTypes.TEXT)
        self.author = DiscourseColumn('author', PropertyTypes.TEXT)
        self.updated_at = DiscourseColumn('updated_at', PropertyTypes.DATETIME)
        self.connection_id = DiscourseColumn('connection_id', PropertyTypes.INT)


class DiscourseColumn(object):
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


# discourse table instances
DISCOURSE_TOPICS_TABLE = DiscourseTopicsTable('0000', 'DiscourseTopics')
DISCOURSE_REPLIES_TABLE = DiscourseRepliesTable('0000', 'DiscourseReplies')

DISCOURSE_TOPICS_COLUMNS = [
    DISCOURSE_TOPICS_TABLE.columns.topic_id.to_dict(),
    DISCOURSE_TOPICS_TABLE.columns.title.to_dict(),
    DISCOURSE_TOPICS_TABLE.columns.slug.to_dict(),
    DISCOURSE_TOPICS_TABLE.columns.views.to_dict(),
    DISCOURSE_TOPICS_TABLE.columns.category_id.to_dict(),
    DISCOURSE_TOPICS_TABLE.columns.connection_id.to_dict(),
    DISCOURSE_TOPICS_TABLE.columns.bumped_at.to_dict(),
    DISCOURSE_TOPICS_TABLE.columns.need_index.to_dict(),
    DISCOURSE_TOPICS_TABLE.columns.deleted.to_dict(),
]

DISCOURSE_REPLIES_COLUMNS = [
    DISCOURSE_REPLIES_TABLE.columns.topic_id.to_dict(),
    DISCOURSE_REPLIES_TABLE.columns.post_number.to_dict(),
    DISCOURSE_REPLIES_TABLE.columns.content.to_dict(),
    DISCOURSE_REPLIES_TABLE.columns.author.to_dict(),
    DISCOURSE_REPLIES_TABLE.columns.updated_at.to_dict(),
    DISCOURSE_REPLIES_TABLE.columns.connection_id.to_dict(),
]
