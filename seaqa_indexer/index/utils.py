import requests
import logging
import datetime
import re
import json
from urllib.parse import urlparse

from seaqa_indexer.utils.constants import CONNECTION_TYPE_TO_INDEX_PREFIX, ConnectionType
from seaqa_indexer.ai_processor.utils import generate_embedding
from seaqa_indexer.utils import uuid_str_to_32_chars
from seaqa_indexer.seadb_models.models import KnowledgeBaseTable, TicketsTable
from seaqa_indexer.handlers import get_handler_or_none, gen_table_name_by_connection_type


logger = logging.getLogger(__name__)

def gen_index_name_by_type(connection_id, connection_type):
    return CONNECTION_TYPE_TO_INDEX_PREFIX.get(connection_type) + str(connection_id)

def gen_kb_index_name(project_uuid):
    return f"knowledge_base_{uuid_str_to_32_chars(project_uuid)}"

def gen_ticket_index_name(project_uuid):
    return f"ticket_{uuid_str_to_32_chars(project_uuid)}"

def gen_conn_summary_index_name(connection_id, connection_type):
    return f"{connection_type}_summary_vector_{connection_id}"

def gen_kb_summary_index_name(project_uuid):
    return f"knowledge_base_summary_vector_{uuid_str_to_32_chars(project_uuid)}"

def gen_ticket_summary_index_name(project_uuid):
    return f"ticket_summary_vector_{uuid_str_to_32_chars(project_uuid)}"

def gen_content_vector_index_name(connection_id, connection_type):
    return f"{connection_type}_content_vector_{connection_id}"

def get_content_by_download_link(download_url):
    response = requests.get(download_url)
    if response.status_code != 200:
        raise ConnectionError(response.status_code, response.text)
    file_content = response.content

    return file_content


def fix_summary(project_uuid, table_name, seadb_api):
    logger.info('Start fixing AI summary embeddings for table %s', table_name)

    try:
        page_size = 100
        fixed_count = 0
        updates = []
        while True:
            sql = f"""
                SELECT _pk, ai_summary
                FROM `{table_name}`
                WHERE ai_summary IS NOT NULL 
                AND ai_summary != ''
                AND ai_summary_vector IS NULL
                AND (deleted = false OR deleted IS NULL)
                LIMIT {page_size}
            """

            result = seadb_api.query_rows(project_uuid, sql)
            rows = result.get('results', [])

            if not rows:
                break

            for row in rows:
                pk = row.get('_pk')
                ai_summary = row.get('ai_summary')

                embedding = generate_embedding(ai_summary, project_uuid)
                if not embedding:
                    logger.warning('Failed to generate embedding for pk=%s', pk)
                    continue

                update_data = {
                    'pk': pk,
                    'row': {
                        'ai_summary_vector': embedding,
                        'ai_processed_time': datetime.datetime.now(datetime.UTC).isoformat()
                    }
                }

                updates.append(update_data)
                if len(updates) >= 50:
                    seadb_api.update_rows(project_uuid, table_name, updates)
                    updates = []

                fixed_count += 1
            if updates:
                seadb_api.update_rows(project_uuid, table_name, updates)

        logger.info('Finished fixing embeddings for table %s, fixed %d records', table_name, fixed_count)
        return fixed_count

    except Exception as e:
        logger.exception('Failed to fix embeddings for table_name %s: %s', table_name, e)
        return 0


class EmailMessage(object):
    """ An email message represents a parsed email body.
    """

    SIG_REGEX = re.compile(r'(--|__|-\w)|(^Sent from my (\w+\s*){1,3})')
    QUOTE_HDR_REGEX = re.compile('On.*wrote:$')
    QUOTED_REGEX = re.compile(r'(>+)')
    HEADER_REGEX = re.compile(r'^\*?(From|Sent|To|Subject):\*? .+')
    _MULTI_QUOTE_HDR_REGEX = r'(?!On.*On\s.+?wrote:)(On\s(.+?)wrote:)'
    MULTI_QUOTE_HDR_REGEX = re.compile(_MULTI_QUOTE_HDR_REGEX, re.DOTALL | re.MULTILINE)
    MULTI_QUOTE_HDR_REGEX_MULTILINE = re.compile(_MULTI_QUOTE_HDR_REGEX, re.DOTALL)

    def __init__(self):
        self.fragments = []
        self.fragment = None
        self.text = ''
        self.found_visible = False

    def read(self, text):
        """ Creates new fragment for each line
            and labels as a signature, quote, or hidden.

            # Returns EmailMessage instance
        """
        if not text:
            return
        self.text = text.replace('\r\n', '\n')

        self.found_visible = False

        is_multi_quote_header = self.MULTI_QUOTE_HDR_REGEX_MULTILINE.search(self.text)
        if is_multi_quote_header:
            self.text = self.MULTI_QUOTE_HDR_REGEX.sub(is_multi_quote_header.groups()[0].replace('\n', ''), self.text)

        # Fix any outlook style replies, with the reply immediately above the signature boundary line
        #   See email_2_2.txt for an example
        self.text = re.sub('([^\n])(?=\n ?[_-]{7,})', '\\1\n', self.text, re.MULTILINE)

        self.lines = self.text.split('\n')
        self.lines.reverse()

        for line in self.lines:
            self._scan_line(line)

        self._finish_fragment()

        self.fragments.reverse()

    @property
    def reply(self):
        """ Captures reply message within email
        """
        reply = []
        for f in self.fragments:
            if not (f.hidden or f.quoted):
                reply.append(f.content)
        return '\n'.join(reply).strip('')

    def _scan_line(self, line):
        """ Reviews each line in email message and determines fragment type

            line - a row of text from an email message
        """
        is_quote_header = self.QUOTE_HDR_REGEX.match(line) is not None
        is_quoted = self.QUOTED_REGEX.match(line) is not None
        is_header = is_quote_header or self.HEADER_REGEX.match(line) is not None

        if self.fragment and len(line.strip()) == 0:
            if self.SIG_REGEX.match(self.fragment.lines[-1].strip()):
                self.fragment.signature = True
                self._finish_fragment()

        if self.fragment \
                and ((self.fragment.headers == is_header and self.fragment.quoted == is_quoted) or
                         (self.fragment.quoted and (is_quote_header or len(line.strip()) == 0))):

            self.fragment.lines.append(line)
        else:
            self._finish_fragment()
            self.fragment = Fragment(is_quoted, line, headers=is_header)

    def quote_header(self, line):
        """ Determines whether line is part of a quoted area

            line - a row of the email message

            Returns True or False
        """
        return self.QUOTE_HDR_REGEX.match(line[::-1]) is not None

    def _finish_fragment(self):
        if self.fragment:
            self.fragment.finish()
            if self.fragment.headers:
                # Regardless of what's been seen to this point, if we encounter a headers fragment,
                # all the previous fragments should be marked hidden and found_visible set to False.
                self.found_visible = False
                for f in self.fragments:
                    f.hidden = True
            if not self.found_visible:
                if self.fragment.quoted \
                        or self.fragment.headers \
                        or self.fragment.signature \
                        or (len(self.fragment.content.strip()) == 0):

                    self.fragment.hidden = True
                else:
                    self.found_visible = True
            self.fragments.append(self.fragment)
        self.fragment = None

    def parse_reply(self, text):
        """ Provides the reply portion of email.
            text - A string email body
        """
        self.read(text)
        return self.reply


class Fragment(object):
    """ A Fragment is a part of
        an Email Message, labeling each part.
    """

    def __init__(self, quoted, first_line, headers=False):
        self.signature = False
        self.headers = headers
        self.hidden = False
        self.quoted = quoted
        self._content = None
        self.lines = [first_line]

    def finish(self):
        """ Creates block of content with lines
            belonging to fragment.
        """
        self.lines.reverse()
        self._content = '\n'.join(self.lines)
        self.lines = None

    @property
    def content(self):
        return self._content.strip()


def get_result_related_seadb_records(project_uuid, all_results, connection_id_to_connection, seadb_api):
    def query_records_by_pks(table_name, pks):
        pks_str = ','.join(map(str, pks))
        sql = f"SELECT * FROM `{table_name}` WHERE _pk IN ({pks_str})"
        res = seadb_api.query_rows(project_uuid, sql)
        return res.get('results', [])

    connection_id_to_pks = {}  # {connection_id: [pk1, pk2]}
    kb_pks = set()
    ticket_pks = set()

    for result in all_results:
        if result.get('source_type') == 'knowledge_base_summary':
            kb_pks.add(result.get('pk'))
        elif result.get('source_type') == 'ticket_summary':
            ticket_pks.add(result.get('pk'))
        else:
            connection_id = int(result.get('connection_id'))
            if connection_id not in connection_id_to_pks:
                connection_id_to_pks[connection_id] = []
            connection_id_to_pks[connection_id].append(result.get('pk'))

    connection_pk_to_record = {}  # {connection_id: {pk: record}}
    for connection_id, connection_pks in connection_id_to_pks.items():
        connection = connection_id_to_connection[connection_id]
        connection_table_name = gen_table_name_by_connection_type(connection.type, connection_id)
        if not connection_table_name:
            logger.warning(f'Table name not found for connection {connection_id}')
            continue

        records = query_records_by_pks(connection_table_name, connection_pks)
        for record in records:
            if connection_id not in connection_pk_to_record:
                connection_pk_to_record[connection_id] = {}
            connection_pk_to_record[connection_id][record.get('_pk')] = record

    kb_pk_to_record = {}
    if kb_pks:
        kb_table_name = KnowledgeBaseTable.gen_table_name()
        records = query_records_by_pks(kb_table_name, kb_pks)
        kb_pk_to_record = {record.get('_pk'): record for record in records}

    ticket_pk_to_record = {}
    if ticket_pks:
        ticket_table_name = TicketsTable.gen_table_name()
        records = query_records_by_pks(ticket_table_name, ticket_pks)
        ticket_pk_to_record = {record.get('_pk'): record for record in records}
    return connection_pk_to_record, kb_pk_to_record, ticket_pk_to_record


def format_vector_search_result(result, connection_id_to_connection, kb_pk_to_record, ticket_pk_to_record, connection_pk_to_record):
    pk = result.get('pk')
    source_type = result.get('source_type')
    score = result.get('score')
    ai_summary = result.get('ai_summary', '')

    formatted_result = {
        '_id': pk,
        'score': score,
        'content': ai_summary,
        'source_type': source_type,
    }

    if source_type == 'knowledge_base_summary':
        record = kb_pk_to_record.get(int(pk))
        formatted_result['type'] = 'knowledge_base'
        formatted_result['title'] = record.get('title', '')
        formatted_result['modified_time'] = record.get('modified_time', '')

    elif source_type == 'ticket_summary':
        record = ticket_pk_to_record.get(int(pk))
        formatted_result['type'] = 'ticket'
        formatted_result['title'] = record.get('title', '')
        formatted_result['modified_time'] = record.get('modified_time', '')

    else:
        # format connection vector search result
        connection_id = result.get('connection_id')
        connection = connection_id_to_connection.get(int(connection_id))
        record = connection_pk_to_record.get(int(connection_id), {}).get(int(pk))
        connection_type = connection.type

        formatted_result['connection_id'] = connection_id
        formatted_result['type'] = connection_type

        conn_handler = get_handler_or_none(connection_type)
        if not conn_handler:
            logger.warning(f'Unsupported connection type: {connection_type}')
            return
        type_specific_result = conn_handler.format_vector_search_result(record, connection, result)
        formatted_result.update(type_specific_result)

    return formatted_result
