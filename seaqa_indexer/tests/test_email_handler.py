import datetime
from unittest.mock import Mock, patch

from seaqa_indexer.handlers.email_handler import EMAIL_OAUTH_CONFIGS, EmailHandler
from seaqa_indexer.utils.mail_viewer import EmailViewError, OAuthMailViewer


class TestEmailHandlerOAuthConfig:
    def test_oauth_configs_contain_credentials_and_token_url_only(self):
        assert set(EMAIL_OAUTH_CONFIGS['Gmail']) == {'client_id', 'client_secret', 'token_url'}
        assert EMAIL_OAUTH_CONFIGS['Gmail']['token_url'] == 'https://oauth2.googleapis.com/token'
        assert set(EMAIL_OAUTH_CONFIGS['Microsoft']) == {'client_id', 'client_secret', 'token_url'}
        assert EMAIL_OAUTH_CONFIGS['Microsoft']['token_url'] == \
            'https://login.microsoftonline.com/common/oauth2/v2.0/token'

    def test_shared_microsoft_uses_connection_credentials_and_tenant_token_url(self):
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
        }

    def test_shared_gmail_uses_connection_credentials_and_default_token_url(self):
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
        }


class TestOAuthMailViewerRefresh:
    def test_refresh_request_does_not_include_scope(self):
        viewer = OAuthMailViewer(
            {'server_provider': 'Gmail'},
            {
                'access_token': 'expired-access-token',
                'refresh_token': 'refresh-token',
                'expires_at': 0,
            },
            {
                'client_id': 'client-id',
                'client_secret': 'client-secret',
                'token_url': 'https://oauth.example.com/token',
            },
        )
        response = Mock(status_code=200)
        response.json.return_value = {
            'access_token': 'new-access-token',
            'expires_in': 3600,
        }

        with patch('seaqa_indexer.utils.mail_viewer.oauth_mail_viewer.requests.post', return_value=response) as post_mock:
            refreshed_token = viewer.refresh_access_token_if_needed()

        assert refreshed_token['access_token'] == 'new-access-token'
        request_data = post_mock.call_args.kwargs['data']
        assert request_data == (
            'grant_type=refresh_token&client_id=client-id&client_secret=client-secret&refresh_token=refresh-token'
        )
        assert 'scope=' not in request_data


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
        mail_viewer.search_emails.side_effect = EmailViewError('sync failed')

        with patch('seaqa_indexer.handlers.email_handler.get_storage'), \
                patch('seaqa_indexer.handlers.email_handler.get_mail_viewer_from_config', return_value=mail_viewer):
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
