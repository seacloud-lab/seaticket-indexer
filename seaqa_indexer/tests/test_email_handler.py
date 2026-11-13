import datetime
import pytest
from unittest.mock import Mock, patch

from seaqa_indexer.handlers.email_handler import EMAIL_OAUTH_CONFIGS, EmailHandler
from seaqa_indexer.utils.constants import ConnectionType
from seaqa_indexer.utils.mail_viewer import EmailViewError, OAuthMailViewer
from seaqa_indexer.utils.mail_viewer.exceptions import EmailAuthProviderError, EmailConfigError


class TestEmailHandlerOAuthConfig:
    @pytest.mark.parametrize('provider,account_type', [
        ('Gmail', 'personal'), ('Microsoft', 'personal'), ('Microsoft', 'shared'),
    ])
    def test_uses_platform_credentials(self, provider, account_type):
        config = {'server_provider': provider, 'account_type': account_type,
                  'sender_email': 'shared@example.com'}
        oauth = Mock(access_token='access', refresh_token='refresh', expires_at=Mock(timestamp=lambda: 1))
        db = Mock()
        db.get_connection_oauth.return_value = oauth
        handler = EmailHandler()
        platform_config = {'client_id': 'platform-id', 'client_secret': 'platform-secret', 'token_url': 'https://provider.example/token'}
        with patch.dict(EMAIL_OAUTH_CONFIGS, {provider: platform_config}), \
                patch('seaqa_indexer.handlers.email_handler.get_mail_viewer_from_config') as viewer, \
                patch.object(handler, '_sync_email', return_value=(0, 0)):
            handler.sync(1, 'project-id', config, None, {}, db, Mock())
        effective = viewer.call_args.args[2]
        assert effective == platform_config

    def test_oauth_configs_contain_credentials_and_token_url_only(self):
        assert set(EMAIL_OAUTH_CONFIGS['Gmail']) == {'client_id', 'client_secret', 'token_url'}
        assert EMAIL_OAUTH_CONFIGS['Gmail']['token_url'] == 'https://oauth2.googleapis.com/token'
        assert set(EMAIL_OAUTH_CONFIGS['Microsoft']) == {'client_id', 'client_secret', 'token_url'}
        assert EMAIL_OAUTH_CONFIGS['Microsoft']['token_url'] == \
            'https://login.microsoftonline.com/common/oauth2/v2.0/token'


class TestOAuthMailViewerRefresh:
    def test_incomplete_config_does_not_log_sensitive_values(self, caplog):
        with pytest.raises(EmailConfigError):
            OAuthMailViewer(
                {'server_provider': 'Gmail'}, {},
                {'client_id': 'sensitive-client-id', 'token_url': 'https://sensitive.example/token'},
            )
        assert 'OAuth mail viewer configuration is incomplete.' in caplog.text
        assert 'sensitive' not in caplog.text

    @pytest.mark.parametrize('operation', ['refresh_access_token_if_needed', '_request_json', '_request_bytes'])
    def test_request_failure_does_not_log_sensitive_details(self, caplog, operation):
        sensitive_value = 'sensitive-oauth-response'
        viewer = OAuthMailViewer(
            {'server_provider': 'Gmail'}, {'refresh_token': sensitive_value, 'expires_at': 0},
            {'client_id': sensitive_value, 'client_secret': sensitive_value,
             'token_url': 'https://sensitive.example/token'},
        )
        response = Mock(status_code=400)
        response.json.return_value = {'error_description': sensitive_value}
        with patch('seaqa_indexer.utils.mail_viewer.oauth_mail_viewer.requests.post', return_value=response), \
                patch('seaqa_indexer.utils.mail_viewer.oauth_mail_viewer.requests.get', return_value=response):
            with pytest.raises((EmailAuthProviderError, EmailViewError)):
                if operation == 'refresh_access_token_if_needed':
                    viewer.refresh_access_token_if_needed()
                else:
                    getattr(viewer, operation)('https://sensitive.example/messages')
        assert 'Failure to fetch' in caplog.text
        assert sensitive_value not in caplog.text
        assert viewer.token_url not in caplog.text
        assert all(record.exc_info is None for record in caplog.records)

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
        seaqa_db.get_project_connection_oauth.return_value = oauth
        sync_logger = Mock()
        handler = EmailHandler()
        mail_viewer = Mock()
        mail_viewer.refresh_access_token_if_needed.return_value = {
            'access_token': 'new-access',
            'refresh_token': 'new-refresh',
            'expires_at': 999999,
        }
        mail_viewer.search_emails.side_effect = EmailViewError('sync failed')

        with patch('seaqa_indexer.handlers.email_handler.get_mail_viewer_from_config', return_value=mail_viewer):
            result = handler.sync(connection_id, project_uuid, config, None, {}, seaqa_db, sync_logger)

        assert result == (None, None, None, None)
        seaqa_db.update_project_connection_oauth.assert_called_once_with(
            project_uuid, ConnectionType.EMAIL.value, 'new-access',
            datetime.datetime.fromtimestamp(999999, tz=datetime.timezone.utc), 'new-refresh', connection_id
        )

    def test_does_not_persist_oauth_when_token_is_still_valid(self):
        connection_id = 1
        config = {'server_provider': 'Gmail'}
        oauth = Mock(access_token='access-token', refresh_token='refresh-token', expires_at=Mock(timestamp=lambda: 1))
        seaqa_db = Mock()
        seaqa_db.get_project_connection_oauth.return_value = oauth
        handler = EmailHandler()
        mail_viewer = Mock()
        mail_viewer.refresh_access_token_if_needed.return_value = None

        with patch('seaqa_indexer.handlers.email_handler.get_mail_viewer_from_config', return_value=mail_viewer), \
                patch.object(handler, '_sync_email', return_value=(0, 0)):
            handler.sync(connection_id, 'project-uuid', config, None, {}, seaqa_db, Mock())

        seaqa_db.update_project_connection_oauth.assert_not_called()
