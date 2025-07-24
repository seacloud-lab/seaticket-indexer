from urllib.parse import urljoin, urlparse
import os
import re
import hashlib
import urllib.parse
from seaqa_indexer.config import CRAWL_OUTPUT_DIR


def url_to_filename(url):
    """
    Convert URL to valid filename
    """
    # Remove protocol prefix
    url = re.sub(r'^https?://', '', url)

    # Replace invalid characters
    filename = urllib.parse.quote_plus(url)
    # filename = unquote(urllib.parse.quote_plus(url))

    # Ensure filename doesn't exceed maximum length limit (255 characters)
    if len(filename) > 240:
        # Keep beginning and end, use hash value in the middle
        hash_part = hashlib.md5(url.encode('utf-8')).hexdigest()[:16]
        filename = filename[:110] + '_' + hash_part + '_' + filename[-110:]

    # Add .json extension
    return filename + '.json'


def get_site_dir(site_id, project_uuid):
    base_dir = os.path.join(CRAWL_OUTPUT_DIR, project_uuid)
    site_dir = os.path.join(base_dir, str(site_id))
    return site_dir
