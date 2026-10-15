import logging
from datetime import datetime, timezone
from email.header import decode_header
from email.parser import BytesParser, Parser
from email.utils import getaddresses, parseaddr, parsedate_to_datetime

from charset_normalizer import from_bytes

from seaqa_indexer.utils import get_no_duplicate_obj_name

logger = logging.getLogger(__name__)


class BasicMailViewer:
    def __init__(self, config):
        self.config = config
        self.config_updated = False

    @staticmethod
    def decode_str(value):
        if not value:
            return ''

        result_str = ''
        for part, charset in decode_header(value):
            if not charset:
                if isinstance(part, bytes):
                    result_str += part.decode(errors='replace')
                    continue
                result_str += part
                continue
            if charset.lower() in ['gb2312', 'gbk', 'gb18030']:
                charset = 'gb18030'
            result_str += part.decode(charset, errors='replace')
        return result_str

    @staticmethod
    def parse_content(part):
        payload = part.get_payload(decode=True)
        if not payload:
            return ''
        guess = from_bytes(payload).best()
        return str(guess) if guess else payload.decode('utf-8', errors='replace')

    def get_content(self, msg):
        plain_content = ''
        html_content = ''
        for part in msg.walk():
            if part.is_multipart():
                continue
            if part.get_filename() is not None:
                continue

            content_type = part.get_content_type()
            if content_type == 'text/plain' and not plain_content:
                plain_content = self.parse_content(part)
            elif content_type == 'text/html' and not html_content:
                html_content = '```' + self.parse_content(part) + '```'

        return plain_content, html_content

    @staticmethod
    def normalize_attachment_filename(filename):
        parts = []
        for part in filename.replace('\\', '/').split('/'):
            if part in ('', '.'):
                continue
            if part == '..':
                if parts:
                    parts.pop()
                continue
            parts.append(part.replace('\x00', '_'))
        return '/'.join(parts) or 'attachment'

    def get_attachments(self, msg):
        file_list = []
        filename2content_id = {}
        exist_file_names = set()
        for part in msg.walk():
            disposition = part.get_content_disposition()
            content_id = part.get('Content-ID')
            filename = part.get_filename()
            content_type = part.get_content_type()

            if filename is not None or (content_id is not None and 'image' in content_type):
                data = part.get_payload(decode=True)
                filename = self.decode_str(filename) if filename else content_id[1:-1] + '.' + content_type[6:]
                filename = self.normalize_attachment_filename(filename)
                filename = get_no_duplicate_obj_name(filename, exist_file_names)
                exist_file_names.add(filename)
                file_list.append({'file_name': filename, 'file_data': data, 'content_disposition': disposition})

                if content_id is not None and 'image' in content_type:
                    filename2content_id[filename] = content_id[1:-1]
        return file_list, filename2content_id

    def parse_addr(self, addr):
        addr_info = parseaddr(addr)
        email_address = addr_info[1]
        if not email_address:
            return self.decode_str(addr)
        try:
            nickname = self.decode_str(addr_info[0])
            if not nickname:
                nickname = email_address.split('@')[0]
            return nickname + ' <' + email_address + '>'
        except Exception as e:
            logger.warning('parse addr: %s addr_info: %s error: %s', addr, addr_info, e)
            return '%s <%s>' % (email_address.split('@')[0], email_address)

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
                    value = ','.join([
                        self.parse_addr(f'{name} <{address}>' if name else address)
                        for name, address in getaddresses([value])
                        if address
                    ])
                elif header in ['Message-ID', 'In-Reply-To']:
                    value = value.strip()
            header_info[header] = value

        return header_info

    @staticmethod
    def format_date(date_value):
        if isinstance(date_value, str):
            date_value = parsedate_to_datetime(date_value)
        if not date_value:
            return datetime.now(timezone.utc).isoformat()
        if date_value.tzinfo is None:
            date_value = date_value.replace(tzinfo=timezone.utc)
        return date_value.astimezone(timezone.utc).isoformat()

    @staticmethod
    def _parse_message(mail_body):
        if isinstance(mail_body, bytes):
            return BytesParser().parsebytes(mail_body)
        return Parser().parsestr(mail_body)

    @staticmethod
    def _merge_extra_fields(email_dict, extra_fields):
        for key, value in extra_fields.items():
            if value is not None:
                email_dict[key] = value
        return email_dict

    def gen_email_dict(self, mail_body, is_sender, **extra_fields):
        msg = self._parse_message(mail_body)

        header_info = self.get_email_header(msg)
        message_id = header_info.get('Message-ID')
        if not message_id:
            logger.error('message-id not found for user: %s', self.config.get('username'))

        if not header_info['From']:
            logger.warning('account: %s message: %s no sender!', self.config.get('username'), message_id)
        if not header_info['To']:
            logger.warning('account: %s message: %s no recipient!', self.config.get('username'), message_id)
        plain_content, html_content = self.get_content(msg)
        file_list, filename2content_id = self.get_attachments(msg)
        email_dict = {
            'content': plain_content,
            'html_content': html_content,
            'filename2content_id': filename2content_id,
            'attachments': file_list,
            'email_from': header_info.get('From'),
            'email_to': header_info.get('To'),
            'subject': header_info.get('Subject'),
            'message_id': header_info.get('Message-ID'),
            'reply_to_message_id': header_info.get('In-Reply-To'),
            'cc': header_info.get('CC'),
            'email_date': self.format_date(header_info.get('Date')),
            'is_sender': is_sender,
        }

        return self._merge_extra_fields(email_dict, extra_fields)

    def search_emails(self, start_date, end_date):
        raise NotImplementedError

    def close(self):
        return None


__all__ = ['BasicMailViewer']
