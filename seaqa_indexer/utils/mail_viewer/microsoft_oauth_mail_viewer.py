from datetime import datetime

from .oauth_mail_viewer import OAuthMailViewer


class MicrosoftOAuthMailViewer(OAuthMailViewer):
    provider_name = 'Microsoft'
    base_url = 'https://graph.microsoft.com/v1.0/me'

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
                '$select': 'id,conversationId',
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
