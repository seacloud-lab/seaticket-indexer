import logging
import ssl
from imapclient import IMAPClient
from imapclient.imapclient import SENT
from email.parser import Parser
from email.header import decode_header
from datetime import datetime
from charset_normalizer import from_bytes
from email.utils import parseaddr, parsedate_to_datetime
from seaqa_indexer.utils import get_no_duplicate_obj_name

logger = logging.getLogger(__name__)


class ImapMail(object):

    _EMAIL_TYPE_MAP = {
        'imap.fastmail.com': 'fastmail',
    }

    def __init__(self, serveraddress, user, passwd, port=None, timeout=None, ssl_context=ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)):
        self.serveraddress = serveraddress
        self.user = user
        self.passwd = passwd
        self.port = port
        self.timeout = timeout
        self.ssl_context = ssl_context
        self.server = None
        self.login()

    @property
    def email_type(self):
        return self._EMAIL_TYPE_MAP.get(self.serveraddress)

    def client(self):
        self.server = IMAPClient(self.serveraddress, self.port, timeout=self.timeout, ssl_context=self.ssl_context)
        self.server._imap.debug = 0
        if '163.com' in self.serveraddress:
            self.server.id_({"name": "IMAPClient", "version": "2.1.0"})
        logger.info('connected success')

    def login(self):
        self.client()
        logger.debug('imap: %s client successfully!', self.serveraddress)
        self.server.login(self.user, self.passwd)
        logger.debug('imap_host: %s email_user: %s, login imap client successfully!', self.serveraddress, self.user)

    @staticmethod
    def decode_str(s):
        result_str = ''
        for value, charset in decode_header(s):
            if not charset:
                if isinstance(value, bytes):
                    result_str += value.decode()
                    continue
                result_str += value
                continue
            if charset.lower() in ['gb2312', 'gbk', 'gb18030']:
                charset = 'gb18030'
            result_str += value.decode(charset)
        return result_str

    @staticmethod
    def parse_content(part):
        payload = part.get_payload(decode=True)
        # The encoding obtained by `part.get_content_charset()` may be incorrect.
        guess = from_bytes(payload).best()
        return str(guess) if guess else payload.decode('utf-8', errors='replace')

    def get_content(self, msg):
        plain_content = ''
        html_content = ''
        for part in msg.walk():
            if part.is_multipart():
                continue
            # if attachment continue
            if part.get_filename() is not None:
                continue

            content_type = part.get_content_type()
            if content_type == 'text/plain':
                plain_content = self.parse_content(part)
            elif content_type == 'text/html':
                html_content = '```' + self.parse_content(part) + '```'

        return plain_content, html_content

    def get_attachments(self, msg):
        file_list = []
        filename2content_id = {}
        exist_file_names = set()
        for part in msg.walk():
            disposition = part.get_content_disposition()
            content_id = part.get('Content-ID')
            filename = part.get_filename()
            content_type = part.get_content_type()

            # filename maybe empty if content-type is image
            if filename is not None or (content_id is not None and 'image' in content_type):
                data = part.get_payload(decode=True)
                filename = self.decode_str(filename) if filename else content_id[1:-1] + '.' + content_type[6:]
                filename = get_no_duplicate_obj_name(filename, exist_file_names)
                exist_file_names.add(filename)
                file_list.append({'file_name': filename, 'file_data': data, 'content_disposition': disposition})

                if content_id is not None and 'image' in content_type:
                    filename2content_id[filename] = content_id[1:-1]
        return file_list, filename2content_id

    def parse_addr(self, addr):
        addr_info = parseaddr(addr)
        try:
            nickname = self.decode_str(addr_info[0])
            if not nickname:
                nickname = addr_info[1].split('@')[0]
            return nickname + ' <' + addr_info[1] + '>'
        except Exception as e:
            logger.warning('parse addr: %s addr_info: %s error: %s', addr, addr_info, e)
            return '%s <%s>' % (addr_info[1].split('@')[0], addr_info[1])

    def get_email_header(self, msg):
        header_info = {}
        for header in ['From', 'To', 'CC', 'Subject', 'Message-ID', 'In-Reply-To', 'Date']:
            value = msg.get(header, '')
            if value:
                if header == 'Subject':
                    value = self.decode_str(value)
                elif header == 'Date':
                    value = parsedate_to_datetime(value)
                elif header in ['From', 'To', 'CC']:
                    value = ','.join([self.parse_addr(val) for val in value.split(',')])
                elif header in ['Message-ID', 'In-Reply-To']:
                    value = value.strip()
            header_info[header] = value

        return header_info

    def gen_email_dict(self, mail_body, is_sender):
        email_dict = {}
        try:
            mail_body = mail_body.decode()
        except UnicodeDecodeError:
            mail_body = mail_body.decode('gb18030')
        msg = Parser().parsestr(mail_body)

        header_info = self.get_email_header(msg)
        message_id = header_info.get('Message-ID')
        if not message_id:
            logger.error('message-id not found, email body: %s', mail_body)

        if not header_info['From']:
            logger.warning('account: %s message: %s no sender!', self.user, message_id)
        if not header_info['To']:
            logger.warning('account: %s message: %s no recipient!', self.user, message_id)
        plain_content, html_content = self.get_content(msg)
        file_list, filename2content_id = self.get_attachments(msg)
        email_dict['content'] = plain_content
        email_dict['html_content'] = html_content
        email_dict['filename2content_id'] = filename2content_id
        email_dict['attachments'] = file_list
        email_dict['email_from'] = header_info.get('From')
        email_dict['email_to'] = header_info.get('To')
        email_dict['subject'] = header_info.get('Subject')
        email_dict['message_id'] = header_info.get('Message-ID')
        email_dict['reply_to_message_id'] = header_info.get('In-Reply-To')
        email_dict['cc'] = header_info.get('CC')
        email_dict['email_date'] = header_info.get('Date').isoformat()
        email_dict['is_sender'] = is_sender

        return email_dict

    def format_imap_date(self, date_str):
        """
        convert datetime to DD-Mon-YYYY
        """

        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        return date_obj.strftime("%d-%b-%Y")

    def search_emails(self, start_date, end_date):
        sent_box = self.server.find_special_folder(SENT)
        inbox = 'INBOX'

        start_date = self.format_imap_date(str(start_date))
        end_date = self.format_imap_date(str(end_date))

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
                    emails = self.server.fetch(email_ids[i: i + offset], ['BODY[]', 'EMAILID', 'THREADID'])
                else:
                    emails = self.server.fetch(email_ids[i: i + offset], ['BODY[]'])
                is_sender = False
                if email_folder == sent_box:
                    is_sender = True

                for uid, message_data in emails.items():
                    mail_body = message_data[b'BODY[]']
                    email_dict = self.gen_email_dict(mail_body, is_sender)
                    if self.email_type == 'fastmail':
                        email_dict['email_id'] = message_data.get(b'EMAILID')[0].decode()
                        email_dict['origin_thread_id'] = message_data.get(b'THREADID')[0].decode()
                    yield email_dict

    def close(self):
        self.server.logout()
