import logging
import datetime
from urllib.parse import urljoin, urlparse
from sqlalchemy.sql import text
from scrapy.linkextractors import LinkExtractor
from scrapy.http import HtmlResponse

from web_crawl_index.db import init_db_session_class
from web_crawl_index.utils import md5

logger = logging.getLogger(__name__)


class URLDiscoveryManager:
    """Manages dynamic URL discovery and interactions with site_urls table"""
    
    def __init__(self):
        self.db_session = init_db_session_class()
        # Use the same LinkExtractor as SiteCrawlerSpider
        self.link_extractor = LinkExtractor()
    
    def add_discovered_url(self, url, site_id, source='dynamic'):
        """Add newly discovered URL to database
        
        Args:
            url: Discovered URL
            site_id: Site ID
            source: Discovery source (sitemap/scrapy/dynamic)
        """
        url_md5 = md5(url)
        
        with self.db_session() as session:
            # Check URL if already exists using MD5
            check_sql = "SELECT id FROM site_urls WHERE url_md5 = :url_md5 AND site_id = :site_id"
            existing = session.execute(text(check_sql), {'url_md5': url_md5, 'site_id': site_id}).fetchone()
            
            if existing:
                logger.debug(f"URL already exists in site_urls: {url} (MD5: {url_md5})")
                return existing.id
            
            # Insert new URL
            insert_sql = """
                INSERT INTO site_urls (url, url_md5, site_id, status, source, discovered_at, created_at, updated_at)
                VALUES (:url, :url_md5, :site_id, :status, :source, :discovered_at, :created_at, :updated_at)
            """
            now = datetime.datetime.now(datetime.UTC)
            params = {
                'url': url,
                'url_md5': url_md5,
                'site_id': site_id,
                'status': 'pending',
                'source': source,
                'discovered_at': now,
                'created_at': now,
                'updated_at': now
            }
            result = session.execute(text(insert_sql), params)
            session.commit()
            logger.info(f"Added new discovered URL: {url} (MD5: {url_md5}, site_id: {site_id})")
            return result.lastrowid
    
    def get_pending_urls(self, site_id=None, limit=100):
        """Get list of pending URLs
        
        Args:
            site_id: Optional site ID
            limit: Return limit
        """
        with self.db_session() as session:
            if site_id:
                sql = """
                    SELECT id, url, url_md5, site_id FROM site_urls 
                    WHERE status = 'pending' AND site_id = :site_id 
                    ORDER BY discovered_at ASC LIMIT :limit
                """
                params = {'site_id': site_id, 'limit': limit}
            else:
                sql = """
                    SELECT id, url, url_md5, site_id FROM site_urls 
                    WHERE status = 'pending' 
                    ORDER BY discovered_at ASC LIMIT :limit
                """
                params = {'limit': limit}
            
            return session.execute(text(sql), params).fetchall()
    
    def get_url_by_md5(self, url_md5, site_id):
        """Get URL by MD5 hash
        
        Args:
            url_md5: URL MD5 hash
            site_id: Site ID
            
        Returns:
            URL record or None
        """
        with self.db_session() as session:
            sql = "SELECT id, url, url_md5, site_id, status FROM site_urls WHERE url_md5 = :url_md5 AND site_id = :site_id"
            return session.execute(text(sql), {'url_md5': url_md5, 'site_id': site_id}).fetchone()
    
    def mark_url_crawling(self, url_id):
        """Mark URL as crawling status"""
        with self.db_session() as session:
            sql = """
                UPDATE site_urls 
                SET status = 'crawling', updated_at = :updated_at 
                WHERE id = :url_id
            """
            session.execute(text(sql), {
                'url_id': url_id,
                'updated_at': datetime.datetime.now(datetime.UTC)
            })
            session.commit()
    
    def mark_url_completed(self, url_id):
        """Mark URL as completed status"""
        with self.db_session() as session:
            sql = """
                UPDATE site_urls 
                SET status = 'completed', crawled_at = :crawled_at, updated_at = :updated_at 
                WHERE id = :url_id
            """
            now = datetime.datetime.now(datetime.UTC)
            session.execute(text(sql), {
                'url_id': url_id,
                'crawled_at': now,
                'updated_at': now
            })
            session.commit()
    
    def mark_url_failed(self, url_id, error_message=None):
        """Mark URL as failed status"""
        with self.db_session() as session:
            sql = """
                UPDATE site_urls 
                SET status = 'failed', error_message = :error_message, updated_at = :updated_at 
                WHERE id = :url_id
            """
            session.execute(text(sql), {
                'url_id': url_id,
                'error_message': error_message,
                'updated_at': datetime.datetime.now(datetime.UTC)
            })
            session.commit()
    
    def extract_links_from_html(self, html_content, base_url, allowed_domains=None):
        """Extract links from HTML content using Scrapy LinkExtractor
        
        Args:
            html_content: HTML content string
            base_url: Base URL for resolving relative links
            allowed_domains: List of allowed domains
        
        Returns:
            List of extracted links
        """
        try:
            # Create Scrapy Response object
            response = HtmlResponse(url=base_url, body=html_content, encoding='utf-8')
            
            # Extract links using LinkExtractor
            links = self.link_extractor.extract_links(response)
            
            extracted_urls = []
            for link in links:
                url = link.url
                
                # Filter by allowed domains if specified
                if allowed_domains:
                    parsed_url = urlparse(url)
                    if parsed_url.netloc not in allowed_domains:
                        continue
                
                extracted_urls.append(url)
            
            logger.debug(f"Extracted {len(extracted_urls)} links from {base_url}")
            return extracted_urls
            
        except Exception as e:
            logger.error(f"Error extracting links from {base_url}: {str(e)}")
            return []
    
    def discover_new_links_from_content(self, html_content, base_url, site_id, allowed_domains=None):
        """Discover new links from page content and add to database
        
        Args:
            html_content: HTML content
            base_url: Page URL
            site_id: Site ID
            allowed_domains: List of allowed domains
        
        Returns:
            Number of newly discovered links
        """
        extracted_urls = self.extract_links_from_html(html_content, base_url, allowed_domains)
        
        new_links_count = 0
        for url in extracted_urls:
            try:
                url_id = self.add_discovered_url(url, site_id, source='dynamic')
                if url_id:
                    new_links_count += 1
            except Exception as e:
                logger.warning(f"Failed to add discovered URL {url}: {str(e)}")
        
        if new_links_count > 0:
            logger.info(f"Discovered {new_links_count} new links from {base_url}")
        
        return new_links_count
