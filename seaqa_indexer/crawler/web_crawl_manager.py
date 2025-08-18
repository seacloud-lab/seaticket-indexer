import logging
import json

from seaqa_indexer.mq import get_mq
from seaqa_indexer.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD



logger = logging.getLogger(__name__)


class WebCrawlManager:
    def __init__(self):
        self.mq = get_mq(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)

    def add_crawl_site_task(self, site_id):
        msg_content = {
            'site_id': site_id,
        }
        self.mq.lpush('web_crawl_task', json.dumps(msg_content))
