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
