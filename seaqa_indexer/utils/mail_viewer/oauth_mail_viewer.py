import logging
import time
from urllib import parse
from email.utils import getaddresses

import requests

from .basic_mail_viewer import BasicMailViewer
from .exceptions import EmailAuthProviderError, EmailConfigError, EmailViewError

logger = logging.getLogger(__name__)


def _check_and_raise_error(response):
    if response.status_code >= 400:
        raise ConnectionError(response.json())


class OAuthMailViewer(BasicMailViewer):
    provider_name = None

    def __init__(self, config, oauth_token=None, oauth_config=None):
        super().__init__(config)
        oauth_token = oauth_token or {}
        oauth_config = oauth_config or config
        self.client_id = oauth_config.get('client_id')
        self.client_secret = oauth_config.get('client_secret')
        self.refresh_token = oauth_token.get('refresh_token')
        self.access_token = oauth_token.get('access_token')
        self.expires_at = oauth_token.get('expires_at')
        self.token_url = oauth_config.get('token_url')
        self.account_type = config.get('account_type') or 'personal'
        self.sender_email = config.get('sender_email')
        provider = self.provider_name or config.get('server_provider')
        supported_types = ('personal', 'alias', 'shared') if provider == 'Microsoft' else ('personal', 'alias')
        if self.account_type not in supported_types:
            raise EmailConfigError('Email account type is invalid')

        if not all([self.client_id, self.client_secret, self.refresh_token, self.token_url]):
            logger.error('OAuth mail viewer config is invalid. client_id: %s, token_url: %s', self.client_id, self.token_url)
            raise EmailConfigError('OAuth mail viewer configuration is incomplete')
        if self.account_type in ('alias', 'shared') and (
                not isinstance(self.sender_email, str) or '@' not in self.sender_email):
            raise EmailConfigError('Alias or shared mailbox sender address is required')

    def refresh_access_token_if_needed(self):
        if not self.access_token or self.expires_at is None or self.expires_at - time.time() < 300:
            params = {
                'grant_type': 'refresh_token',
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'refresh_token': self.refresh_token
            }
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
            }
            response = requests.post(self.token_url, headers=headers, data=parse.urlencode(params))
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

            return self._update_access_token(
                response.get('access_token'),
                expires_at,
                response.get('refresh_token') or self.refresh_token,
            )

    def _update_access_token(self, new_access_token, new_expires_at, new_refresh_token):
        self.access_token = new_access_token
        self.expires_at = new_expires_at
        self.refresh_token = new_refresh_token
        return {
            'access_token': new_access_token,
            'expires_at': new_expires_at,
            'refresh_token': new_refresh_token,
        }

    def _get_headers(self, extra_headers=None):
        headers = {
            'Authorization': f'Bearer {self.access_token}',
        }
        if extra_headers:
            headers.update(extra_headers)
        return headers

    def _request_json(self, url, params=None, headers=None):
        response = requests.get(url, params=params, headers=self._get_headers(headers))
        try:
            _check_and_raise_error(response)
        except Exception as e:
            logger.exception('Failure to fetch %s message json, error: %s', self.provider_name, e)
            raise EmailViewError(f'Failed to fetch {self.provider_name} message json')
        return response.json()

    def _request_bytes(self, url, params=None, headers=None):
        response = requests.get(url, params=params, headers=self._get_headers(headers))
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

    def is_associated_message(self, msg, message_ref):
        if self.account_type == 'personal':
            return True
        headers = []
        for key in ('From', 'To', 'Cc', 'Bcc', 'Delivered-To', 'X-Original-To'):
            headers.extend(msg.get_all(key, []))
        addresses = {address.strip().casefold() for _, address in getaddresses(headers) if address}
        addresses.update(address.strip().casefold() for address in message_ref.get('participant_addresses', []) if address)
        return self.sender_email.strip().casefold() in addresses

    def search_emails(self, start_date, end_date):
        for message_ref in self.iter_message_refs(start_date, end_date):
            mail_body = self.fetch_raw_message(message_ref)
            msg = self._parse_message(mail_body)
            if not self.is_associated_message(msg, message_ref):
                continue
            email_dict = self.gen_email_dict_from_message(
                msg,
                message_ref.get('is_sender', False),
                **self.get_provider_email_metadata(message_ref),
            )
            yield email_dict


__all__ = ['OAuthMailViewer']
