from seaqa_indexer.handlers.base_handler import ConnectionHandler
from seaqa_indexer.handlers.registry import (
    ConnectionHandlerRegistry,
    get_handler,
)
from seaqa_indexer.handlers.site_handler import SiteHandler
from seaqa_indexer.handlers.discourse_handler import DiscourseHandler
from seaqa_indexer.handlers.github_handler import GithubIssueHandler
from seaqa_indexer.handlers.seafile_handler import SeafileHandler
from seaqa_indexer.handlers.email_handler import EmailHandler

ConnectionHandlerRegistry.register(SiteHandler)
ConnectionHandlerRegistry.register(DiscourseHandler)
ConnectionHandlerRegistry.register(GithubIssueHandler)
ConnectionHandlerRegistry.register(SeafileHandler)
ConnectionHandlerRegistry.register(EmailHandler)


__all__ = [
    'ConnectionHandler',
    'ConnectionHandlerRegistry',
    'get_handler',
    'SiteHandler',
    'DiscourseHandler',
    'GithubIssueHandler',
    'SeafileHandler',
    'EmailHandler',
]
