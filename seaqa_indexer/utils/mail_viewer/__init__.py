import logging

from .basic_mail_viewer import BasicMailViewer
from .exceptions import EmailAuthProviderError, EmailConfigError, EmailViewError
from .gmail_oauth_mail_viewer import GmailOAuthMailViewer
from .imap_mail_viewer import ImapMailViewer
from .microsoft_oauth_mail_viewer import MicrosoftOAuthMailViewer
from .oauth_mail_viewer import OAuthMailViewer

logger = logging.getLogger(__name__)


def get_mail_viewer_from_config(config):
    server_provider = config.get('server_provider', 'general_email_provider')

    if server_provider == 'general_email_provider':
        return ImapMailViewer(config)
    if server_provider == 'Gmail':
        return GmailOAuthMailViewer(config)
    if server_provider == 'Microsoft':
        return MicrosoftOAuthMailViewer(config)

    logger.error('Invalid server_provider: %s', server_provider)
    raise EmailConfigError(f'Invalid server_provider: {server_provider}')


__all__ = [
    'BasicMailViewer',
    'EmailAuthProviderError',
    'EmailConfigError',
    'EmailViewError',
    'GmailOAuthMailViewer',
    'ImapMailViewer',
    'MicrosoftOAuthMailViewer',
    'OAuthMailViewer',
    'get_mail_viewer_from_config',
]
