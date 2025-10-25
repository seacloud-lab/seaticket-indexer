# coding: UTF-8

import os
import logging
import re
import chardet
import json
from zipfile import ZipFile
from io import BytesIO

from seaqa_indexer.utils.constants import text_suffixes, office_suffixes
from seaqa_indexer.utils.converter import pptx_to_markdown
from seaqa_indexer.utils.converter import pdf_to_markdown
from seaqa_indexer.utils.converter import docx_to_markdown
from seaqa_indexer.utils.converter import sdoc_to_markdown


logger = logging.getLogger(__name__)


class ZipString(ZipFile):
    def __init__(self, content):
        ZipFile.__init__(self, BytesIO(content))


def extract_pdf_text(content):
    pdf_md = pdf_to_markdown(BytesIO(content))
    return pdf_md.encode()


def extract_docx_text(content):
    md_content = docx_to_markdown(BytesIO(content))
    return md_content.encode()


def extract_pptx_text(content):
    pptx_md = pptx_to_markdown(BytesIO(content))
    return pptx_md.encode()


def extract_odf_text(content):
    doc = ZipString(content)
    content = doc.read('content.xml')
    cleaned = re.sub('<(.|\n)*?>', ' ', content.decode())
    return cleaned.encode()


def extract_sdoc_text(content):
    data = json.loads(content)
    markdown_text = sdoc_to_markdown(data)
    return markdown_text.encode()

EXTRACT_TEXT_FUNCS = {
    'docx': extract_docx_text,
    'pptx': extract_pptx_text,
    'pdf': extract_pdf_text,
    'odt': extract_odf_text,
    'ods': extract_odf_text,
    'odp': extract_odf_text,
    'sdoc': extract_sdoc_text,
}

EXTRACT_TEXT_FUNCS.update(dict([(suffix, lambda content, *args: content) for suffix in text_suffixes]))

def get_file_suffix(path):
    try:
        name = os.path.basename(path)
        suffix = os.path.splitext(name)[1][1:]
        if suffix:
            return suffix.lower()
        return None
    except:
        return None


def is_text_file(path):
    suffix = get_file_suffix(path)
    if not suffix:
        return False

    if suffix in text_suffixes:
        return True

    return False

def is_office_pdf(path):
    suffix = get_file_suffix(path)
    if not suffix:
        return False

    if suffix in office_suffixes:
        return True

    return False

def is_text_office(filename):
    return is_text_file(filename) or is_office_pdf(filename)


class Extractor(object):
    def __init__(self, func):
        self.func = func

    def extract(self, connection_id, repo_id, content, path):
        try:
            logger.info('connection: %s, extracting %s %s...', connection_id, repo_id, path)
            content = self.func(content)
            logger.info('connection: %s, successfully extracted %s', connection_id, path)
        except Exception as e:
            logger.warning('connection: %s,failed to extract %s: %s', connection_id, path, e)
            return None

        return self.fix_encoding(repo_id, path, content)

    def fix_encoding(self, repo_id, path, content):
        if not content:
            return None
        enc = chardet.detect(content[:4000]).get('encoding', None)
        if not enc:
            logger.warning('%s %s: encoding is unknown', repo_id, path)
            return None
        enc = enc.lower()

        try:
            content = content.decode(enc).encode('utf-8').decode('utf-8')
        except Exception as e:
            logger.warning('%s: %s failed to trans code from %s to utf-8, because: %s', repo_id, path, enc, e)
            return None

        return content


class ExtractorFactory(object):
    @classmethod
    def get_extractor(cls, filename):
        suffix = get_file_suffix(filename)
        func = EXTRACT_TEXT_FUNCS.get(suffix, None)
        if not func:
            return None
        return Extractor(func)
