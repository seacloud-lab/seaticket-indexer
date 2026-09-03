from seaqa_indexer.handlers.base_handler import ConnectionHandler
from seaqa_indexer.handlers.confluence_handler import ConfluenceHandler
from seaqa_indexer.handlers.notion_handler import NotionHandler
from seaqa_indexer.handlers.registry import (
    ConnectionHandlerRegistry,
    get_handler,
)
from seaqa_indexer.handlers.site_handler import SiteHandler
from seaqa_indexer.handlers.discourse_handler import DiscourseHandler
from seaqa_indexer.handlers.github_handler import GithubIssueHandler
from seaqa_indexer.handlers.linear_handler import LinearIssueHandler
from seaqa_indexer.handlers.jira_handler import JiraIssueHandler
from seaqa_indexer.handlers.seafile_handler import SeafileHandler
from seaqa_indexer.handlers.email_handler import EmailHandler
from seaqa_indexer.handlers.general_task_handler import GeneralTaskHandler
from seaqa_indexer.handlers.discord_handler import DiscordHandler
from seaqa_indexer.handlers.slack_handler import SlackHandler

ConnectionHandlerRegistry.register(SiteHandler)
ConnectionHandlerRegistry.register(DiscourseHandler)
ConnectionHandlerRegistry.register(GithubIssueHandler)
ConnectionHandlerRegistry.register(LinearIssueHandler)
ConnectionHandlerRegistry.register(JiraIssueHandler)
ConnectionHandlerRegistry.register(SeafileHandler)
ConnectionHandlerRegistry.register(EmailHandler)
ConnectionHandlerRegistry.register(NotionHandler)
ConnectionHandlerRegistry.register(ConfluenceHandler)
ConnectionHandlerRegistry.register(GeneralTaskHandler)
ConnectionHandlerRegistry.register(DiscordHandler)
ConnectionHandlerRegistry.register(SlackHandler)


__all__ = [
    'ConnectionHandler',
    'ConnectionHandlerRegistry',
    'get_handler',
    'SiteHandler',
    'DiscourseHandler',
    'GithubIssueHandler',
    'LinearIssueHandler',
    'JiraIssueHandler',
    'SeafileHandler',
    'EmailHandler',
    'NotionHandler',
    'ConfluenceHandler',
    'GeneralTaskHandler',
    'DiscordHandler',
    'SlackHandler',
]
