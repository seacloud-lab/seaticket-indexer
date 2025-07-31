import requests
from scrapy.http import HtmlResponse

class UpdateCheckMiddleware:
    def process_request(self, request, spider):
        if not hasattr(spider, 'existing_cache_info') or not spider.existing_cache_info:
            return None

        url = request.url
        if url in spider.existing_cache_info:
            cache_info = spider.existing_cache_info[url]

            try:
                head_response = requests.head(url, timeout=10, allow_redirects=True)
                response_etag = head_response.headers.get('ETag')
                response_last_modified = head_response.headers.get('Last-Modified')

                content_changed = False
                if response_etag and cache_info.get('etag') != response_etag:
                    content_changed = True
                elif response_last_modified and cache_info.get('last_modified') != response_last_modified:
                    content_changed = True
                
                if not content_changed:
                    spider.logger.debug(f"No changes detected for {url}, skipping download")
                    # Add the skipped URL to the set on the spider instance
                    if hasattr(spider, 'not_modified_urls'):
                        spider.not_modified_urls.add(url)
                    return HtmlResponse(url=url, status=200, request=request, flags=['not_modified'])

            except requests.RequestException as e:
                spider.logger.warning(f"HEAD request failed for {url}: {e}, proceeding with full download")

        return None
