import datetime
from unittest.mock import Mock, patch

from seaqa_indexer.handlers.email_handler import EMAIL_OAUTH_CONFIGS, EmailHandler
from seaqa_indexer.utils.mail_viewer import EmailViewError


class TestEmailHandlerOAuthConfig:
    def test_oauth_scopes_include_mailbox_write_permissions(self):
        assert EMAIL_OAUTH_CONFIGS['Gmail']['scopes']['personal'][0] == \
            'https://www.googleapis.com/auth/gmail.modify'
        assert EMAIL_OAUTH_CONFIGS['Gmail']['scopes']['shared'][0] == \
            'https://www.googleapis.com/auth/gmail.modify'
        assert 'Mail.ReadWrite' in EMAIL_OAUTH_CONFIGS['Microsoft']['scopes']['personal']
        assert 'Mail.ReadWrite.Shared' in EMAIL_OAUTH_CONFIGS['Microsoft']['scopes']['shared']

    def test_shared_microsoft_uses_trusted_scopes_and_tenant_token_url(self):
        connection_id = 1
        config = {
            'server_provider': 'Microsoft',
            'account_type': 'shared',
            'client_id': 'shared-client-id',
            'client_secret': 'shared-client-secret',
            'sender_email': 'shared@example.com',
            'token_url': 'https://login.microsoftonline.com/tenant-id/oauth2/v2.0/token',
        }
        oauth = Mock(access_token='access-token', refresh_token='refresh-token', expires_at=Mock(timestamp=lambda: 1))
        seaqa_db = Mock()
        seaqa_db.get_connection_oauth.return_value = oauth
        sync_logger = Mock()
        handler = EmailHandler()
        mail_viewer = Mock()
        mail_viewer.refresh_access_token_if_needed.return_value = None

        with patch('seaqa_indexer.handlers.email_handler.get_storage'), \
                patch('seaqa_indexer.handlers.email_handler.get_mail_viewer_from_config', return_value=mail_viewer) as viewer_mock, \
                patch.object(handler, '_sync_email', return_value=(0, 0)):
            handler.sync(connection_id, 'project-uuid', config, None, {}, seaqa_db, sync_logger)

        oauth_config = viewer_mock.call_args.args[2]
        assert oauth_config == {
            'client_id': 'shared-client-id',
            'client_secret': 'shared-client-secret',
            'token_url': 'https://login.microsoftonline.com/tenant-id/oauth2/v2.0/token',
            'scopes': EMAIL_OAUTH_CONFIGS['Microsoft']['scopes']['shared'],
        }

    def test_shared_gmail_uses_trusted_scopes_without_config_scopes(self):
        connection_id = 1
        config = {
            'server_provider': 'Gmail',
            'account_type': 'shared',
            'client_id': 'shared-client-id',
            'client_secret': 'shared-client-secret',
            'sender_email': 'shared@example.com',
        }
        oauth = Mock(access_token='access-token', refresh_token='refresh-token', expires_at=Mock(timestamp=lambda: 1))
        seaqa_db = Mock()
        seaqa_db.get_connection_oauth.return_value = oauth
        sync_logger = Mock()
        handler = EmailHandler()
        mail_viewer = Mock()
        mail_viewer.refresh_access_token_if_needed.return_value = None

        with patch('seaqa_indexer.handlers.email_handler.get_storage'), \
                patch('seaqa_indexer.handlers.email_handler.get_mail_viewer_from_config', return_value=mail_viewer) as viewer_mock, \
                patch.object(handler, '_sync_email', return_value=(0, 0)):
            handler.sync(connection_id, 'project-uuid', config, None, {}, seaqa_db, sync_logger)

        oauth_config = viewer_mock.call_args.args[2]
        assert oauth_config == {
            'client_id': 'shared-client-id',
            'client_secret': 'shared-client-secret',
            'token_url': EMAIL_OAUTH_CONFIGS['Gmail']['token_url'],
            'scopes': EMAIL_OAUTH_CONFIGS['Gmail']['scopes']['shared'],
        }


class TestEmailHandlerOAuthRefresh:
    def test_persists_refreshed_oauth_before_email_sync(self):
        connection_id = 1
        project_uuid = 'project-uuid'
        config = {'server_provider': 'Gmail'}
        oauth = Mock(access_token='old-access', refresh_token='old-refresh', expires_at=Mock(timestamp=lambda: 1))
        seaqa_db = Mock()
        seaqa_db.get_connection_oauth.return_value = oauth
        sync_logger = Mock()
        handler = EmailHandler()
        mail_viewer = Mock()
        mail_viewer.refresh_access_token_if_needed.return_value = {
            'access_token': 'new-access',
            'refresh_token': 'new-refresh',
            'expires_at': 999999,
        }

        with patch('seaqa_indexer.handlers.email_handler.get_storage'), \
                patch('seaqa_indexer.handlers.email_handler.get_mail_viewer_from_config', return_value=mail_viewer), \
                patch.object(handler, '_sync_email', side_effect=EmailViewError('sync failed')):
            result = handler.sync(connection_id, project_uuid, config, None, {}, seaqa_db, sync_logger)

        assert result == (None, None, None, None)
        seaqa_db.update_connection_oauth.assert_called_once_with(
            project_uuid, connection_id, 'new-access',
            datetime.datetime.fromtimestamp(999999, tz=datetime.timezone.utc), 'new-refresh'
        )

    def test_does_not_persist_oauth_when_token_is_still_valid(self):
        connection_id = 1
        config = {'server_provider': 'Gmail'}
        oauth = Mock(access_token='access-token', refresh_token='refresh-token', expires_at=Mock(timestamp=lambda: 1))
        seaqa_db = Mock()
        seaqa_db.get_connection_oauth.return_value = oauth
        handler = EmailHandler()
        mail_viewer = Mock()
        mail_viewer.refresh_access_token_if_needed.return_value = None

        with patch('seaqa_indexer.handlers.email_handler.get_storage'), \
                patch('seaqa_indexer.handlers.email_handler.get_mail_viewer_from_config', return_value=mail_viewer), \
                patch.object(handler, '_sync_email', return_value=(0, 0)):
            handler.sync(connection_id, 'project-uuid', config, None, {}, seaqa_db, Mock())

        seaqa_db.update_connection_oauth.assert_not_called()
