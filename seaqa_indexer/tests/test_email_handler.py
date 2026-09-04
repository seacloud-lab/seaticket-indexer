from unittest.mock import Mock, patch

from seaqa_indexer.handlers.email_handler import EMAIL_OAUTH_CONFIGS, EmailHandler


class TestEmailHandlerOAuthConfig:
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
        mail_viewer = Mock(oauth_updated=False)

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
        mail_viewer = Mock(oauth_updated=False)

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
