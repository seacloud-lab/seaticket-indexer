import base64
from datetime import datetime, timezone
from email.utils import getaddresses

from .oauth_mail_viewer import OAuthMailViewer


class GmailOAuthMailViewer(OAuthMailViewer):
    provider_name = 'Gmail'

    @staticmethod
    def _normalize_associated_address(address):
        address = address.strip().casefold()
        local_part, separator, domain = address.rpartition('@')
        if separator and domain in ('gmail.com', 'googlemail.com'):
            # Consumer Gmail ignores dots and plus tags; Workspace domains do not
            # necessarily share these rules.
            local_part = local_part.split('+', 1)[0].replace('.', '')
            return f'{local_part}@gmail.com'
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
    def list_url(self):
        return 'https://gmail.googleapis.com/gmail/v1/users/me/messages'

    @staticmethod
    def format_search_date(date_value):
        date_obj = datetime.strptime(str(date_value), '%Y-%m-%d')
        return int(datetime.combine(date_obj.date(), datetime.min.time(), tzinfo=timezone.utc).timestamp())

    def _build_query(self, mailbox, start_date, end_date):
        label = 'in:sent' if mailbox == 'sent' else 'in:inbox'
        return (
            f'{label} '
            f'after:{self.format_search_date(start_date)} '
            f'before:{self.format_search_date(end_date)}'
        )

    def iter_message_refs(self, start_date, end_date):
        for mailbox, is_sender in [('inbox', False), ('sent', True)]:
            page_token = None
            while True:
                params = {
                    'q': self._build_query(mailbox, start_date, end_date),
                    'maxResults': 500,
                }
                if page_token:
                    params['pageToken'] = page_token
                response = self._request_json(self.list_url, params=params)
                for message in response.get('messages', []):
                    yield {
                        'id': message.get('id'),
                        'email_id': message.get('id'),
                        'origin_thread_id': message.get('threadId'),
                        'is_sender': is_sender,
                        'mailbox': mailbox,
                    }
                page_token = response.get('nextPageToken')
                if not page_token:
                    break

    def fetch_raw_message(self, message_ref):
        response = self._request_json(
            f"{self.list_url}/{message_ref['id']}",
            params={'format': 'raw'},
        )
        raw_message = response.get('raw', '')
        return base64.urlsafe_b64decode(raw_message + '=' * (-len(raw_message) % 4))

    def get_provider_email_metadata(self, message_ref):
        return {
            'email_id': message_ref.get('email_id'),
            'origin_thread_id': message_ref.get('origin_thread_id'),
        }


__all__ = ['GmailOAuthMailViewer']
