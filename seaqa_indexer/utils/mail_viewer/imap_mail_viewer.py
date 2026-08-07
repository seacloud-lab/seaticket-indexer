import logging
import ssl
from datetime import datetime

from imapclient import IMAPClient
from imapclient.imapclient import SENT

from .basic_mail_viewer import BasicMailViewer
from .exceptions import EmailConfigError, EmailViewError

logger = logging.getLogger(__name__)


class ImapMailViewer(BasicMailViewer):

    _EMAIL_TYPE_MAP = {
        'imap.fastmail.com': 'fastmail',
    }

    def __init__(self, config, timeout=None, ssl_context=ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)):
        super().__init__(config)
        self.serveraddress = config.get('imap_host')
        self.user = config.get('username')
        self.passwd = config.get('password')
        self.port = config.get('imap_port')
        self.timeout = timeout
        self.ssl_context = ssl_context
        self.server = None

        if not all([self.serveraddress, self.user, self.passwd]):
            logger.error('Mail viewer config is invalid. imap_host: %s, username: %s', self.serveraddress, self.user)
            raise EmailConfigError('Mail viewer configuration is incomplete')

        self.login()

    @property
    def email_type(self):
        return self._EMAIL_TYPE_MAP.get(self.serveraddress)

    def client(self):
        try:
            self.server = IMAPClient(self.serveraddress, self.port, timeout=self.timeout, ssl_context=self.ssl_context)
            self.server._imap.debug = 0
            if '163.com' in self.serveraddress:
                self.server.id_({'name': 'IMAPClient', 'version': '2.1.0'})
            logger.info('connected success')
        except Exception as e:
            logger.exception('Failed to connect IMAP client. host: %s, error: %s', self.serveraddress, e)
            raise EmailViewError('Failed to connect to IMAP server')

    def login(self):
        self.client()
        try:
            logger.debug('imap: %s client successfully!', self.serveraddress)
            self.server.login(self.user, self.passwd)
            logger.debug('imap_host: %s email_user: %s, login imap client successfully!', self.serveraddress, self.user)
        except Exception as e:
            logger.exception('Email server authorization failed. host: %s, user: %s, error: %s', self.serveraddress, self.user, e)
            raise EmailConfigError('Email server authorization failed')

    def format_search_date(self, date_str):
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        return date_obj.strftime('%d-%b-%Y')

    def search_emails(self, start_date, end_date):
        sent_box = self.server.find_special_folder(SENT)
        inbox = 'INBOX'

        start_date = self.format_search_date(str(start_date))
        end_date = self.format_search_date(str(end_date))

        for email_folder in [inbox, sent_box]:
            logger.debug('start to get user: %s emails from box: %s', self.user, email_folder)
            try:
                self.server.select_folder(email_folder, readonly=True)
            except Exception as e:
                logger.warning('user: %s select email folder: %s error: %s', self.user, email_folder, e)
                continue
            search_criteria = f'(SINCE "{start_date}" BEFORE "{end_date}")'
            email_ids = self.server.search(search_criteria)

            offset = 10
            for i in range(0, len(email_ids), offset):
                if self.email_type == 'fastmail':
                    emails = self.server.fetch(email_ids[i: i + offset], ['BODY[]', 'FLAGS', 'EMAILID', 'THREADID'])
                else:
                    emails = self.server.fetch(email_ids[i: i + offset], ['BODY[]', 'FLAGS'])
                is_sender = email_folder == sent_box

                for _uid, message_data in emails.items():
                    mail_body = message_data[b'BODY[]']
                    extra_fields = {}
                    # Parse IMAP FLAGS to extract \Answered
                    flags = message_data.get(b'FLAGS', ())
                    if any(b'\\Answered' in flag for flag in flags):
                        extra_fields['answered'] = True
                    if self.email_type == 'fastmail':
                        extra_fields['email_id'] = message_data.get(b'EMAILID')[0].decode()
                        extra_fields['origin_thread_id'] = message_data.get(b'THREADID')[0].decode()
                    email_dict = self.gen_email_dict(mail_body, is_sender, **extra_fields)
                    yield email_dict

    def close(self):
        if not self.server:
            return
        try:
            self.server.logout()
        except Exception as e:
            logger.warning('Failed to logout IMAP client for user %s: %s', self.user, e)
