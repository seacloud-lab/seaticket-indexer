import logging
import time
from urllib import parse

import requests

from .basic_mail_viewer import BasicMailViewer
from .exceptions import EmailAuthProviderError, EmailConfigError, EmailViewError

logger = logging.getLogger(__name__)


def _check_and_raise_error(response):
    if response.status_code >= 400:
        raise ConnectionError(response.json())


class OAuthMailViewer(BasicMailViewer):
    provider_name = None
    request_timeout = None

    def __init__(self, config):
        super().__init__(config)
        self.client_id = config.get('client_id')
        self.client_secret = config.get('client_secret')
        self.refresh_token = config.get('refresh_token')
        self.access_token = config.get('access_token')
        self.expires_at = config.get('expires_at')
        self.token_url = config.get('token_url')
        self.scopes = config.get('scopes')

        if not all([self.client_id, self.client_secret, self.refresh_token, self.token_url, self.scopes]):
            logger.error('OAuth mail viewer config is invalid. client_id: %s, token_url: %s', self.client_id, self.token_url)
            raise EmailConfigError('OAuth mail viewer configuration is incomplete')

        self.session = requests.Session()

    def _request_access_token(self):
        if not self.access_token or self.expires_at is None or self.expires_at - time.time() < 300:
            params = {
                'grant_type': 'refresh_token',
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'refresh_token': self.refresh_token,
                'scope': ' '.join(self.scopes),
            }
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
            }
            response = self.session.post(self.token_url, headers=headers, data=parse.urlencode(params), timeout=self.request_timeout)
            try:
                _check_and_raise_error(response)
            except Exception as e:
                logger.exception('Failure to fetch new access token, error: %s', e)
                raise EmailAuthProviderError('Failed to fetch access token')
            else:
                response = response.json()
                if 'access_token' not in response:
                    logger.exception('Failure to fetch new access token. No access_token in response.')
                    raise EmailAuthProviderError('No access_token in response')

            expires_at = 0
            if response.get('ext_expires_at'):
                expires_at = response.get('ext_expires_at')
            elif response.get('expires_at'):
                expires_at = response.get('expires_at')
            elif response.get('ext_expires_in'):
                expires_at = response.get('ext_expires_in') + time.time()
            elif response.get('expires_in'):
                expires_at = response.get('expires_in') + time.time()

            self._update_access_token(
                response.get('access_token'),
                expires_at,
                response.get('refresh_token') or self.refresh_token,
            )

    def _update_access_token(self, new_access_token, new_expires_at, new_refresh_token):
        self.access_token = new_access_token
        self.expires_at = new_expires_at
        self.refresh_token = new_refresh_token
        self.config['access_token'] = new_access_token
        self.config['expires_at'] = new_expires_at
        self.config['refresh_token'] = new_refresh_token
        self.config_updated = True

    def _get_headers(self, extra_headers=None):
        headers = {
            'Authorization': f'Bearer {self.access_token}',
        }
        if extra_headers:
            headers.update(extra_headers)
        return headers

    def _request_json(self, url, params=None, headers=None):
        response = self.session.get(url, params=params, headers=self._get_headers(headers), timeout=self.request_timeout)
        try:
            _check_and_raise_error(response)
        except Exception as e:
            logger.exception('Failure to fetch %s message json, error: %s', self.provider_name, e)
            raise EmailViewError(f'Failed to fetch {self.provider_name} message json')
        return response.json()

    def _request_bytes(self, url, params=None, headers=None):
        response = self.session.get(url, params=params, headers=self._get_headers(headers), timeout=self.request_timeout)
        try:
            _check_and_raise_error(response)
        except Exception as e:
            logger.exception('Failure to fetch %s raw message, error: %s', self.provider_name, e)
            raise EmailViewError(f'Failed to fetch {self.provider_name} raw message')
        return response.content

    def iter_message_refs(self, start_date, end_date):
        raise NotImplementedError

    def fetch_raw_message(self, message_ref):
        raise NotImplementedError

    def get_provider_email_metadata(self, message_ref):
        return {}

    def close(self):
        self.session.close()

    def search_emails(self, start_date, end_date):
        self._request_access_token()
        for message_ref in self.iter_message_refs(start_date, end_date):
            mail_body = self.fetch_raw_message(message_ref)
            email_dict = self.gen_email_dict(
                mail_body,
                message_ref.get('is_sender', False),
                **self.get_provider_email_metadata(message_ref),
            )
            yield email_dict


__all__ = ['OAuthMailViewer']
