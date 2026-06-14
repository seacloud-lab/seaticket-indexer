from seaqa_indexer.seadb_models.schema_loader import SCHEMA


class SchemaTables:
    WEB_CRAWL = SCHEMA.WebCrawlTable
    GITHUB_ISSUES = SCHEMA.GithubIssuesTable
    GITHUB_ISSUE_COMMENTS = SCHEMA.GithubIssueCommentsTable
    DISCOURSE_TOPICS = SCHEMA.DiscourseTopicsTable
    DISCOURSE_REPLIES = SCHEMA.DiscourseRepliesTable
    SEAFILE = SCHEMA.SeafileTable
    TICKETS = SCHEMA.TicketsTable
    TICKET_COMMENTS = SCHEMA.TicketCommentsTable
    TICKET_ACTIVITIES = SCHEMA.TicketActivitiesTable
    EMAIL = SCHEMA.EmailTable
    THREAD = SCHEMA.ThreadTable
    KNOWLEDGE_BASE = SCHEMA.KnowledgeBaseTable
    TAG = SCHEMA.TagTable
    PORTAL_ISSUES = SCHEMA.PortalIssuesTable
    PORTAL_ISSUE_COMMENTS = SCHEMA.PortalIssueCommentsTable
    AGENT_RUNS = SCHEMA.AgentRunsTable
    AGENT_ACTIONS = SCHEMA.AgentActionsTable
    NOTION = SCHEMA.NotionTable
    GENERAL_TASK = SCHEMA.GeneralTaskTable
    CONNECTION_USER = SCHEMA.ConnectionUserTable
    LINEAR_ISSUES = SCHEMA.LinearIssuesTable
    LINEAR_ISSUE_COMMENTS = SCHEMA.LinearIssueCommentsTable
    CONFLUENCE = SCHEMA.ConfluenceTable
    DISCORD_THREADS = SCHEMA.DiscordThreadsTable
    DISCORD_THREAD_MESSAGES = SCHEMA.DiscordThreadMessagesTable
    JIRA_ISSUES = SCHEMA.JiraIssuesTable
    JIRA_ISSUE_COMMENTS = SCHEMA.JiraIssueCommentsTable


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

class SelectTypes:
    ticket_status = {
      "options": [
        {
          "id": "0001",
          "name": "open",
          "color": "#1A7F37",
          "text_color": "#FFFFFF",
        },
        {
          "id": "0002",
          "name": "closed",
          "color": "#8250DF",
          "text_color": "#FFFFFF",
        }
      ]
    }
    ticket_substate = {
      "cascade_column_key": "",
      "cascade_settings": {
        "0001": [
          "0010",
          "0011",
          "0012",
          "0013"
        ],
        "0002": [
          "0014",
          "0015",
          "0016"
        ]
      },
      "options": [
        {
          "id": "0010",
          "name": "New",
          "color": "#59CB74",
          "text_color": "#FFFFFF",
        },
        {
          "id": "0011",
          "name": "Working on",
          "color": "#46A1FD",
          "text_color": "#FFFFFF",
        },
        {
          "id": "0012",
          "name": "Backlog",
          "color": "#9C9C9E",
          "text_color": "#FFFFFF",
        },
        {
          "id": "0013",
          "name": "Waiting on user",
          "color": "#EAA775",
          "text_color": "#FFFFFF"
        },
        {
          "id": "0014",
          "name": "Completed",
          "color": "#8250DF",
          "text_color": "#FFFFFF",
        },
        {
          "id": "0015",
          "name": "Not planned",
          "color": "#59636E",
          "text_color": "#FFFFFF"
        },
        {
          "id": "0016",
          "name": "Duplicate",
          "color": "#59636E",
          "text_color": "#FFFFFF"
        },
      ]
    }

    state = {
      "options": [
        {
          "id": "0001",
          "name": "open",
          "color": "#1A7F37",
          "text_color": "#FFFFFF"
        },
        {
          "id": "0002",
          "name": "closed",
          "color": "#8250DF",
          "text_color": "#FFFFFF"
        }
      ]
    }

    state_reason = {
      "options": [
        {
          "id": "0001",
          "name": "completed",
          "color": "#8250DF",
          "text_color": "#FFFFFF"
        },
        {
          "id": "0002",
          "name": "not_planned",
          "color": "#59636E",
          "text_color": "#FFFFFF"
        },
        {
          "id": "0003",
          "name": "duplicate",
          "color": "#59636E",
          "text_color": "#FFFFFF"
        },
        {
          "id": "0004",
          "name": "reopened",
          "color": "#1A7F37",
          "text_color": "#FFFFFF"
        }
      ]
    }

    ticket_type = {
      "options": [
        {
          "id": "0001",
          "name": "Bug",
          "color": "#ffebe9",
          "text_color": "#000000"
        },
        {
          "id": "0002",
          "name": "Feature",
          "color": "#ddf4ff",
          "text_color": "#000000"
        },
        {
          "id": "0003",
          "name": "Question",
          "color": "#fff8c5",
          "text_color": "#000000"
        }
      ]
    }

    issue_type = {
      "options": [
        {
          "id": "0001",
          "name": "Bug",
          "color": "#ffebe9",
          "text_color": "#d1242f"
        },
        {
          "id": "0002",
          "name": "Feature",
          "color": "#ddf4ff",
          "text_color": "#0969da"
        },
        {
          "id": "0003",
          "name": "Task",
          "color": "#fff8c5",
          "text_color": "#9a6700"
        }
      ]
    }

    jira_priority = {
      "options": []
    }

    jira_status = {
      "options": []
    }

    jira_issue_type = {
      "options": []
    }

    jira_assignee = {
      "options": []
    }


class MappedColumn(object):
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


class BaseModel:
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls._meta = {
            'fields': []
        }

        for name, attr in cls.__dict__.items():
            if isinstance(attr, MappedColumn):
                cls._meta['fields'].append(attr)

    @classmethod
    def get_fields(cls):
        return cls._meta['fields'].copy()

    def __init__(self, **kwargs):
        for field in self.__class__._meta['fields']:
            setattr(self, field.name, field)

    def __repr__(self):
        fields = ", ".join([f"{k}={v!r}" for k, v in self.__dict__.items()])
        return f"{self.__class__.__name__}({fields})"


class DiscourseTopicsTable(BaseModel):
    topic_id = MappedColumn('topic_id', PropertyTypes.INT)
    title = MappedColumn('title', PropertyTypes.TEXT)
    slug = MappedColumn('slug', PropertyTypes.TEXT)
    views = MappedColumn('views', PropertyTypes.INT)
    category_id = MappedColumn('category_id', PropertyTypes.INT)
    modified_time = MappedColumn('modified_time', PropertyTypes.DATETIME)
    resolved = MappedColumn('resolved', PropertyTypes.BOOL)
    deleted = MappedColumn('deleted', PropertyTypes.BOOL)
    outdated = MappedColumn('outdated', PropertyTypes.BOOL)
    record_modified_time = MappedColumn('record_modified_time', PropertyTypes.DATETIME)
    sync_time = MappedColumn('sync_time', PropertyTypes.DATETIME)
    created_time = MappedColumn('created_time', PropertyTypes.DATETIME)
    ai_summary = MappedColumn('ai_summary', PropertyTypes.TEXT)
    ai_processed_time = MappedColumn('ai_processed_time', PropertyTypes.DATETIME)
    ai_summary_vector = MappedColumn('ai_summary_vector', PropertyTypes.LIST, ListTypes.vector)

    @classmethod
    def gen_table_name(cls, connection_id):
        return ConnectionType.DISCOURSE_FORUM.value + '_' + str(connection_id)


class DiscourseRepliesTable(BaseModel):
    topic_id = MappedColumn('topic_id', PropertyTypes.INT)
    post_number = MappedColumn('post_number', PropertyTypes.INT)
    content = MappedColumn('content', PropertyTypes.TEXT, {'compressed': True})
    author = MappedColumn('author', PropertyTypes.TEXT)
    modified_time = MappedColumn('modified_time', PropertyTypes.DATETIME)
    accepted_answer = MappedColumn('accepted_answer', PropertyTypes.BOOL)

    @classmethod
    def gen_table_name(cls, connection_id):
        return ConnectionType.DISCOURSE_FORUM.value + '_replies' + '_' + str(connection_id)


class WebCrawlTable(BaseModel):
    url = MappedColumn('url', PropertyTypes.TEXT)
    title = MappedColumn('title', PropertyTypes.TEXT)
    etag = MappedColumn('etag', PropertyTypes.TEXT)
    modified_time = MappedColumn('modified_time', PropertyTypes.DATETIME)
    sync_time = MappedColumn('sync_time', PropertyTypes.DATETIME)
    deleted = MappedColumn('deleted', PropertyTypes.BOOL)
    outdated = MappedColumn('outdated', PropertyTypes.BOOL)
    record_modified_time = MappedColumn('record_modified_time', PropertyTypes.DATETIME)
    hash = MappedColumn('hash', PropertyTypes.TEXT)
    ai_summary = MappedColumn('ai_summary', PropertyTypes.TEXT)
    ai_processed_time = MappedColumn('ai_processed_time', PropertyTypes.DATETIME)
    ai_summary_vector = MappedColumn('ai_summary_vector', PropertyTypes.LIST, ListTypes.vector)

    @classmethod
    def gen_table_name(cls, connection_id):
        return ConnectionType.SITE.value + '_' + str(connection_id)


class GithubIssuesTable(BaseModel):
    issue_id = MappedColumn('issue_id', PropertyTypes.INT)
    issue_number = MappedColumn('issue_number', PropertyTypes.INT)
    title = MappedColumn('title', PropertyTypes.TEXT)
    content = MappedColumn('content', PropertyTypes.TEXT, {'compressed': True})
    state = MappedColumn('state', PropertyTypes.SINGLE_SELECT, SelectTypes.state)
    state_reason = MappedColumn('state_reason', PropertyTypes.SINGLE_SELECT, SelectTypes.state_reason)
    labels = MappedColumn('labels', PropertyTypes.MULTIPLE_SELECT)
    issue_type = MappedColumn('issue_type', PropertyTypes.SINGLE_SELECT, SelectTypes.issue_type)
    author = MappedColumn('author', PropertyTypes.TEXT)
    assignees = MappedColumn('assignees', PropertyTypes.TEXT)
    url = MappedColumn('url', PropertyTypes.TEXT)
    created_time = MappedColumn('created_time', PropertyTypes.DATETIME)
    modified_time = MappedColumn('modified_time', PropertyTypes.DATETIME)
    sync_time = MappedColumn('sync_time', PropertyTypes.DATETIME)
    closed_time = MappedColumn('closed_time', PropertyTypes.DATETIME)
    comment_count = MappedColumn('comment_count', PropertyTypes.INT)
    deleted = MappedColumn('deleted', PropertyTypes.BOOL)
    outdated = MappedColumn('outdated', PropertyTypes.BOOL)
    record_modified_time = MappedColumn('record_modified_time', PropertyTypes.DATETIME)
    ai_summary = MappedColumn('ai_summary', PropertyTypes.TEXT)
    ai_processed_time = MappedColumn('ai_processed_time', PropertyTypes.DATETIME)
    ai_summary_vector = MappedColumn('ai_summary_vector', PropertyTypes.LIST, ListTypes.vector)
    linked_ticket = MappedColumn('linked_ticket', PropertyTypes.INT)

    @classmethod
    def gen_table_name(cls, connection_id):
        return ConnectionType.GITHUB_ISSUE.value + '_' + str(connection_id)


class GithubIssueCommentsTable(BaseModel):
    comment_id = MappedColumn('comment_id', PropertyTypes.INT)
    issue_id = MappedColumn('issue_id', PropertyTypes.INT)
    author = MappedColumn('author', PropertyTypes.TEXT)
    content = MappedColumn('content', PropertyTypes.TEXT, {'compressed': True})
    created_time = MappedColumn('created_time', PropertyTypes.DATETIME)
    modified_time = MappedColumn('modified_time', PropertyTypes.DATETIME)

    @classmethod
    def gen_table_name(cls, connection_id):
        return ConnectionType.GITHUB_ISSUE.value + '_comments' + '_' + str(connection_id)


class JiraIssuesTable(BaseModel):
    issue_id = MappedColumn('issue_id', PropertyTypes.INT)
    issue_key = MappedColumn('issue_key', PropertyTypes.TEXT)
    title = MappedColumn('title', PropertyTypes.TEXT)
    content = MappedColumn('content', PropertyTypes.TEXT, {'compressed': True})
    status = MappedColumn('status', PropertyTypes.SINGLE_SELECT, SelectTypes.jira_status)
    priority = MappedColumn('priority', PropertyTypes.SINGLE_SELECT, SelectTypes.jira_priority)
    assignees = MappedColumn('assignees', PropertyTypes.SINGLE_SELECT, SelectTypes.jira_assignee)
    issue_type = MappedColumn('issue_type', PropertyTypes.SINGLE_SELECT, SelectTypes.jira_issue_type)
    due_date = MappedColumn('due_date', PropertyTypes.DATETIME)
    created_time = MappedColumn('created_time', PropertyTypes.DATETIME)
    modified_time = MappedColumn('modified_time', PropertyTypes.DATETIME)
    sync_time = MappedColumn('sync_time', PropertyTypes.DATETIME)
    deleted = MappedColumn('deleted', PropertyTypes.BOOL)
    record_modified_time = MappedColumn('record_modified_time', PropertyTypes.DATETIME)
    outdated = MappedColumn('outdated', PropertyTypes.BOOL)
    ai_summary = MappedColumn('ai_summary', PropertyTypes.TEXT)
    ai_processed_time = MappedColumn('ai_processed_time', PropertyTypes.DATETIME)
    ai_summary_vector = MappedColumn('ai_summary_vector', PropertyTypes.LIST, ListTypes.vector)
    linked_ticket = MappedColumn('linked_ticket', PropertyTypes.INT)

    @classmethod
    def gen_table_name(cls, connection_id):
        return ConnectionType.JIRA_ISSUE.value + '_' + str(connection_id)


class JiraIssueCommentsTable(BaseModel):
    comment_id = MappedColumn('comment_id', PropertyTypes.INT)
    issue_id = MappedColumn('issue_id', PropertyTypes.INT)
    issue_key = MappedColumn('issue_key', PropertyTypes.TEXT)
    author = MappedColumn('author', PropertyTypes.TEXT)
    content = MappedColumn('content', PropertyTypes.TEXT, {'compressed': True})
    created_time = MappedColumn('created_time', PropertyTypes.DATETIME)
    modified_time = MappedColumn('modified_time', PropertyTypes.DATETIME)

    @classmethod
    def gen_table_name(cls, connection_id):
        return ConnectionType.JIRA_ISSUE.value + '_comments' + '_' + str(connection_id)


class SeafileTable(BaseModel):
    path = MappedColumn('path', PropertyTypes.TEXT)
    title = MappedColumn('title', PropertyTypes.TEXT)
    modified_time = MappedColumn('modified_time', PropertyTypes.DATETIME)
    content = MappedColumn('content', PropertyTypes.TEXT, {'compressed': True})
    sync_time = MappedColumn('sync_time', PropertyTypes.DATETIME)
    deleted = MappedColumn('deleted', PropertyTypes.BOOL)
    outdated = MappedColumn('outdated', PropertyTypes.BOOL)
    record_modified_time = MappedColumn('record_modified_time', PropertyTypes.DATETIME)
    ai_summary = MappedColumn('ai_summary', PropertyTypes.TEXT)
    ai_processed_time = MappedColumn('ai_processed_time', PropertyTypes.DATETIME)
    ai_summary_vector = MappedColumn('ai_summary_vector', PropertyTypes.LIST, ListTypes.vector)

    @classmethod
    def gen_table_name(cls, connection_id):
        return ConnectionType.SEAFILE.value + '_' + str(connection_id)


class TicketsTable(BaseModel):
    title = MappedColumn('title', PropertyTypes.TEXT)
    content = MappedColumn('content', PropertyTypes.TEXT)
    ai_summary = MappedColumn('ai_summary', PropertyTypes.TEXT)
    ai_processed_time = MappedColumn('ai_processed_time', PropertyTypes.DATETIME)
    ai_summary_vector = MappedColumn('ai_summary_vector', PropertyTypes.LIST, ListTypes.vector)
    state = MappedColumn('state', PropertyTypes.SINGLE_SELECT, data=SelectTypes.ticket_status)
    substate = MappedColumn('substate', PropertyTypes.SINGLE_SELECT, data=SelectTypes.ticket_substate)
    type = MappedColumn('type', PropertyTypes.SINGLE_SELECT, data=SelectTypes.ticket_type)
    tags = MappedColumn('tags', PropertyTypes.MULTIPLE_SELECT)
    assignees = MappedColumn('assignees', PropertyTypes.LIST)
    participants = MappedColumn('participants', PropertyTypes.LIST)
    priority = MappedColumn('priority', PropertyTypes.INT)
    creator = MappedColumn('creator', PropertyTypes.TEXT)
    comment_count = MappedColumn('comment_count', PropertyTypes.INT)
    created_time = MappedColumn('created_time', PropertyTypes.DATETIME)
    modified_time = MappedColumn('modified_time', PropertyTypes.DATETIME)
    closed_time = MappedColumn('closed_time', PropertyTypes.DATETIME)
    deleted = MappedColumn('deleted', PropertyTypes.BOOL)

    @classmethod
    def gen_table_name(cls):
        return 'tickets'


class TicketCommentsTable(BaseModel):
    ticket_id = MappedColumn('ticket_id', PropertyTypes.INT)
    content = MappedColumn('content', PropertyTypes.TEXT)
    creator = MappedColumn('creator', PropertyTypes.TEXT)
    created_time = MappedColumn('created_time', PropertyTypes.DATETIME)
    modified_time = MappedColumn('modified_time', PropertyTypes.DATETIME)
    deleted = MappedColumn('deleted', PropertyTypes.BOOL)
    via_agent = MappedColumn('via_agent', PropertyTypes.BOOL)

    @classmethod
    def gen_table_name(cls):
        return 'ticket_comments'


class EmailTable(BaseModel):
    email_from = MappedColumn('email_from', PropertyTypes.TEXT)
    email_to = MappedColumn('email_to', PropertyTypes.TEXT)
    message_id = MappedColumn('message_id', PropertyTypes.TEXT)
    title = MappedColumn('title', PropertyTypes.TEXT)
    cc = MappedColumn('cc', PropertyTypes.TEXT)
    content = MappedColumn('content', PropertyTypes.TEXT, {'compressed': True})
    text_content = MappedColumn('text_content', PropertyTypes.TEXT, {'compressed': True})
    html_content = MappedColumn('html_content', PropertyTypes.TEXT, {'compressed': True})
    modified_time = MappedColumn('modified_time', PropertyTypes.DATETIME)
    reply_to_message_id = MappedColumn('reply_to_message_id', PropertyTypes.TEXT)
    attachments = MappedColumn('attachments', PropertyTypes.LIST)
    is_sender = MappedColumn('is_sender', PropertyTypes.BOOL)
    sync_time = MappedColumn('sync_time', PropertyTypes.DATETIME)
    deleted = MappedColumn('deleted', PropertyTypes.BOOL)
    thread_id = MappedColumn('thread_id', PropertyTypes.INT)
    email_id = MappedColumn('email_id', PropertyTypes.TEXT)
    origin_thread_id = MappedColumn('origin_thread_id', PropertyTypes.TEXT)

    @classmethod
    def gen_table_name(cls, connection_id):
        return ConnectionType.EMAIL.value + '_' + str(connection_id)


class ThreadTable(BaseModel):
    title = MappedColumn('title', PropertyTypes.TEXT)
    modified_time = MappedColumn('modified_time', PropertyTypes.DATETIME)
    unread = MappedColumn('unread', PropertyTypes.BOOL)
    sync_time = MappedColumn('sync_time', PropertyTypes.DATETIME)
    deleted = MappedColumn('deleted', PropertyTypes.BOOL)
    tags = MappedColumn('tags', PropertyTypes.LIST, ListTypes.int)
    outdated = MappedColumn('outdated', PropertyTypes.BOOL)
    record_modified_time = MappedColumn('record_modified_time', PropertyTypes.DATETIME)
    ai_summary = MappedColumn('ai_summary', PropertyTypes.TEXT)
    ai_processed_time = MappedColumn('ai_processed_time', PropertyTypes.DATETIME)
    ai_summary_vector = MappedColumn('ai_summary_vector', PropertyTypes.LIST, ListTypes.vector)

    @classmethod
    def gen_table_name(cls, connection_id):
        return ConnectionType.EMAIL.value + '_' + 'thread_' + str(connection_id)


class KnowledgeBaseTable(BaseModel):
    title = MappedColumn('title', PropertyTypes.TEXT)
    content = MappedColumn('content', PropertyTypes.TEXT, {'compressed': True})
    ai_summary = MappedColumn('ai_summary', PropertyTypes.TEXT)
    ai_processed_time = MappedColumn('ai_processed_time', PropertyTypes.DATETIME)
    ai_summary_vector = MappedColumn('ai_summary_vector', PropertyTypes.LIST, ListTypes.vector)
    creator = MappedColumn('creator', PropertyTypes.TEXT)
    created_time = MappedColumn('created_time', PropertyTypes.DATETIME)
    last_modifier = MappedColumn('last_modifier', PropertyTypes.TEXT)
    modified_time = MappedColumn('modified_time', PropertyTypes.DATETIME)
    deleted = MappedColumn('deleted', PropertyTypes.BOOL)

    @classmethod
    def gen_table_name(cls):
        return 'knowledge_base'


class NotionTable(BaseModel):
    title = MappedColumn('title', PropertyTypes.TEXT)
    content = MappedColumn('content', PropertyTypes.TEXT, {'compressed': True})
    creator = MappedColumn('creator', PropertyTypes.TEXT)
    last_modifier = MappedColumn('last_modifier', PropertyTypes.TEXT)
    ai_summary = MappedColumn('ai_summary', PropertyTypes.TEXT)
    parent_page_id = MappedColumn('parent_page_id', PropertyTypes.TEXT)
    modified_time = MappedColumn('modified_time', PropertyTypes.DATETIME)
    sync_time = MappedColumn('sync_time', PropertyTypes.DATETIME)
    record_modified_time = MappedColumn('record_modified_time', PropertyTypes.DATETIME)
    outdated = MappedColumn('outdated', PropertyTypes.BOOL)
    created_time = MappedColumn('created_time', PropertyTypes.DATETIME)
    page_id = MappedColumn('page_id', PropertyTypes.TEXT)
    deleted = MappedColumn('deleted', PropertyTypes.BOOL)
    ai_processed_time = MappedColumn('ai_processed_time', PropertyTypes.DATETIME)
    ai_summary_vector = MappedColumn('ai_summary_vector', PropertyTypes.LIST, ListTypes.vector)

    @classmethod
    def gen_table_name(cls, connection_id):
        return ConnectionType.NOTION.value + '_' + str(connection_id)
