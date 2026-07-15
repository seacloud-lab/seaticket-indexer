import logging
from copy import copy

from trafilatura.core import trafilatura_sequence
from trafilatura.settings import Extractor
from trafilatura.xml import xmltotxt
from trafilatura.utils import load_html
from trafilatura.htmlprocessing import tree_cleaning, convert_tags


logger = logging.getLogger(__name__)

MIN_EXTRACTED_SIZE = '200'


def normalize_postbody(postbody):
    for item in postbody.xpath('.//item'):
        if len(item) == 0 and item.text:
            item.text = ' '.join(item.text.split())

    return postbody


def extract_html(url, file_content, favor_precision=False, favor_recall=False, formatting=True,
                 include_links=True, include_images=False, include_tables=True, fast=False):

    options = Extractor(
        precision=favor_precision,
        recall=favor_recall,
        formatting=formatting,
        links=include_links,
        images=include_images,
        tables=include_tables,
        url=url,
        fast=fast,
    )
    options.config.set('DEFAULT', 'MIN_EXTRACTED_SIZE', MIN_EXTRACTED_SIZE)

    tree = load_html(file_content)
    cleaned_tree = tree_cleaning(copy(tree), options)

    # convert tags, the rest does not work without conversion
    cleaned_tree = convert_tags(cleaned_tree, options, options.url)

    cleaned_tree_backup = copy(cleaned_tree)
    postbody, temp_text, len_text = trafilatura_sequence(cleaned_tree, cleaned_tree_backup, tree, options)
    postbody = normalize_postbody(postbody)

    return xmltotxt(postbody, options.formatting)
