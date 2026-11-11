from datetime import datetime
from email.utils import getaddresses
from urllib.parse import quote

from .oauth_mail_viewer import OAuthMailViewer


class MicrosoftOAuthMailViewer(OAuthMailViewer):
    provider_name = 'Microsoft'

    @staticmethod
    def _normalize_associated_address(address):
        address = address.strip().casefold()
        local_part, separator, domain = address.rpartition('@')
        if separator and domain in ('outlook.com', 'hotmail.com', 'live.com', 'msn.com'):
            # Only normalize known consumer domains. Exchange tenants may disable
            # plus addressing or resolve the full plus address to another mailbox.
            local_part = local_part.split('+', 1)[0]
            return f'{local_part}@{domain}'
        return address

    def is_associated_message(self, msg, message_ref):
        if self.account_type == 'personal':
            return True
        headers = []
        for key in ('From', 'To', 'Cc', 'Bcc', 'Delivered-To', 'X-Original-To'):
            headers.extend(msg.get_all(key, []))
        addresses = {self._normalize_associated_address(address)
                     for _, address in getaddresses(headers) if address}
        addresses.update(self._normalize_associated_address(address)
                         for address in message_ref.get('participant_addresses', []) if address)
        return self._normalize_associated_address(self.sender_email) in addresses

    @property
    def base_url(self):
        principal = f'users/{quote(self.sender_email, safe="")}' if self.account_type == 'shared' else 'me'
        return f'https://graph.microsoft.com/v1.0/{principal}'

    @staticmethod
    def format_search_date(date_value):
        return datetime.strptime(str(date_value), '%Y-%m-%d').isoformat() + 'Z'

    def _mailbox_url(self, mailbox):
        return f'{self.base_url}/mailFolders/{mailbox}/messages'

    def _date_filter(self, field_name, start_date, end_date):
        return (
            f'{field_name} ge {self.format_search_date(start_date)} '
            f'and {field_name} lt {self.format_search_date(end_date)}'
        )

    def iter_message_refs(self, start_date, end_date):
        mailboxes = [
            ('inbox', 'receivedDateTime', False),
            ('sentitems', 'sentDateTime', True),
        ]
        for mailbox, date_field, is_sender in mailboxes:
            next_url = self._mailbox_url(mailbox)
            params = {
                '$select': 'id,conversationId,from,toRecipients,ccRecipients,bccRecipients',
                '$filter': self._date_filter(date_field, start_date, end_date),
                '$top': 100,
            }
            while next_url:
                response = self._request_json(next_url, params=params)
                for message in response.get('value', []):
                    yield {
                        'id': message.get('id'),
                        'email_id': message.get('id'),
                        'origin_thread_id': message.get('conversationId'),
                        'is_sender': is_sender,
                        'mailbox': mailbox,
                        'participant_addresses': [
                            recipient.get('emailAddress', {}).get('address', '')
                            for recipient in ([message.get('from') or {}] +
                                              message.get('toRecipients', []) +
                                              message.get('ccRecipients', []) +
                                              message.get('bccRecipients', []))
                        ],
                    }
                next_url = response.get('@odata.nextLink')
                params = None

    def fetch_raw_message(self, message_ref):
        return self._request_bytes(f"{self.base_url}/messages/{message_ref['id']}/$value")

    def get_provider_email_metadata(self, message_ref):
        return {
            'email_id': message_ref.get('email_id'),
            'origin_thread_id': message_ref.get('origin_thread_id'),
        }


__all__ = ['MicrosoftOAuthMailViewer']
