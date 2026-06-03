from seaqa_indexer.utils.constants import ConnectionType


class SchemaTableNames:
    """Schema table keys used in table_schemas.yaml, for get_table_name_from_schema / get_column_name_from_schema."""
    WEB_CRAWL = 'WebCrawlTable'
    GITHUB_ISSUES = 'GithubIssuesTable'
    GITHUB_ISSUE_COMMENTS = 'GithubIssueCommentsTable'
    DISCOURSE_TOPICS = 'DiscourseTopicsTable'
    DISCOURSE_REPLIES = 'DiscourseRepliesTable'
    SEAFILE = 'SeafileTable'
    TICKETS = 'TicketsTable'
    TICKET_COMMENTS = 'TicketCommentsTable'
    TICKET_ACTIVITIES = 'TicketActivitiesTable'
    EMAIL = 'EmailTable'
    THREAD = 'ThreadTable'
    KNOWLEDGE_BASE = 'KnowledgeBaseTable'
    TAG = 'TagTable'
    PORTAL_ISSUES = 'PortalIssuesTable'
    PORTAL_ISSUE_COMMENTS = 'PortalIssueCommentsTable'
    AGENT_RUNS = 'AgentRunsTable'
    AGENT_ACTIONS = 'AgentActionsTable'
    NOTION = 'NotionTable'
    GENERAL_TASK = 'GeneralTaskTable'
    GENERAL_TASK_USER = 'GeneralTaskUserTable'


class PropertyTypes:
    TEXT = 'text'
    DATETIME = 'datetime'
    INT = 'int64'
    FLOAT = 'float64'
    SINGLE_SELECT = 'single-select'
    MULTIPLE_SELECT = 'multiple-select'
    BOOL = 'bool'
    IMAGE = 'image'
    DATE = 'date'
    LONG_TEXT = 'long-text'
    CHECKBOX = 'checkbox'
    URL = 'url'
    DURATION = 'duration'
    NUMBER = 'number'
    FILE = 'file'
    COLLABORATOR = 'collaborator'
    EMAIL = 'email'
    FORMULA = 'formula'
    CREATOR = 'creator'
    LAST_MODIFIER = 'last-modifier'
    AUTO_NUMBER = 'auto-number'
    LINK = 'link'
    LINK_FORMULA = 'link-formula'
    RATE = 'rate'
    GEOLOCATION = 'geolocation'
    BUTTON = 'button'
    LIST = 'list'


class ListTypes:
    vector = {
        "list_type": "float32",
    }
    int = {
        "list_type": "int64",
    }
