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
    GENERAL_TASK = 'general_task'
    SITE = 'site'
    SEAFILE = 'seafile'
    NOTION = 'notion'
    LINEAR = 'linear'
    CONFLUENCE = 'confluence'
    DISCORD = 'discord'

    @classmethod
    def is_valid(cls, value):
        return value in {item.value for item in cls}

    @classmethod
    def to_list(cls):
        return [
            item.value
            for item in cls
        ]


CONNECTION_TYPE_TO_INDEX_PREFIX = {
    connection_type.value: connection_type.value + '_' for connection_type in ConnectionType
}

class AIScenario(Enum):
    SUMMARY = 'summary'
    AGENT = 'agent'
    CHAT = 'chat'
    SEARCH = 'search'
    RECORD_GENERATION = 'record_generation'
    UNKNOWN = 'unknown'

    @classmethod
    def is_valid(cls, value):
        return value in {item.value for item in cls}

class DataEventType(Enum):
    GITHUB_ISSUE_ADDED = 'github_issue_added'
    GITHUB_ISSUE_UPDATED = 'github_issue_updated'
    GITHUB_ISSUE_CLOSED = 'github_issue_closed'
    GITHUB_ISSUE_REOPENED = 'github_issue_reopened'
    GITHUB_ISSUE_COMMENT_ADDED = 'github_issue_comment_added'
    DISCOURSE_TOPIC_ADDED = 'discourse_topic_added'
    DISCOURSE_TOPIC_UPDATED = 'discourse_topic_updated'
    DISCOURSE_TOPIC_COMMENT_ADDED = 'discourse_topic_comment_added'
    EMAIL_THREAD_ADDED = 'email_thread_added'
    EMAIL_MESSAGE_ADDED = 'email_message_added'
    GENERAL_TASK_ADDED = 'general_task_added'
    GENERAL_TASK_UPDATED = 'general_task_updated'
    CONFLUENCE_PAGE_ADDED = 'confluence_page_added'
    CONFLUENCE_PAGE_UPDATED = 'confluence_page_updated'
    DISCORD_MESSAGE_ADDED = 'discord_message_added'
    DISCORD_MESSAGE_UPDATED = 'discord_message_updated'

    @classmethod
    def is_valid(cls, value):
        return value in {item.value for item in cls}

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

SUPPORT_VECTOR_RETRIEVAL_TYPES = {
    ConnectionType.SITE.value,
    ConnectionType.SEAFILE.value,
}

SEASEARCH_BULK_OPERATE_LIMIT = 100

# Connection
GENERAL_TASK_CONNECTION_SPEC = {
    "task_id": {
        "type": "string",
        "description": "srouce data id。"
    },
    "url": {
        "type": "string",
        "description": "Source task URL."
    },
    "title": {
        "type": "string",
        "description": "task title。"
    },
    "status": {
        "type": "string",
        "description": "The current life cycle status of the task",
        "option": ['new', 'in_progress', 'done', 'canceled'],
    },
    "priority": {
        "type": "string",
        "description": "The urgency or importance level of the task",
        "option": ['high', 'medium', 'low'],
    },
    "assignees": {
        "type": "list<string>",
        "description": "List of unique identifiers for task owners."
    },
    "participants": {
        "type": "list<string>",
        "description": "List of users participating in the task."
    },
    "size": {
        "type": "string",
        "description": "Task workload evaluation value or complexity measurement",
        "option": ['large', 'medium', 'small'],
    },
    "modified_time": {
        "type": "string",
        "description": "The last modified standard UTC time string,"
    },
    "created_time": {
        "type": "string",
        "description": "The standard UTC time string created by the task."
    },
    "due_date": {
        "type": "string",
        "description": "The standard UTC time string for the task deadline or planned completion."
    },
    "description": {
        "type": "string",
        "description": "Detailed description, body or notes of the task."
    },
    "others": {
        "type": "json",
        "description": "Additional structured data for the task."
    },
    "version": {
        "type": "string",
        "description": "Version of the task."
    }
}
