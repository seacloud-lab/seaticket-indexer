import logging
import datetime
import hashlib
from urllib.parse import urlparse
from sqlalchemy.sql import text
from scrapy.linkextractors import LinkExtractor
from scrapy.http import HtmlResponse
from web_crawl_index.db import init_db_session_class

logger = logging.getLogger(__name__)

def md5(text):
    return hashlib.md5(text.encode('utf-8')).hexdigest()

class URLDiscoveryManager:
    """Manages dynamic URL discovery and interactions with site_urls table"""
    
    def __init__(self):
        self.db_session = init_db_session_class()

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
            check_sql = "SELECT id, status FROM site_urls WHERE url_md5 = :url_md5 AND site_id = :site_id"
            existing = session.execute(text(check_sql), {'url_md5': url_md5, 'site_id': site_id}).fetchone()
            
            if existing:
                if existing.status == 'removed':
                    reactivate_sql = """
                        UPDATE site_urls 
                        SET status = 'pending', source = :source, updated_at = :updated_at
                        WHERE id = :url_id
                    """
                    session.execute(text(reactivate_sql), {
                        'url_id': existing.id,
                        'source': source,
                        'updated_at': datetime.datetime.now(datetime.UTC)
                    })
                    session.commit()
                    logger.info(f"Reactivated removed URL: {url} (MD5: {url_md5})")
                    return existing.id
                else:
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
    
    def get_urls_by_domain_from_site(self, site_id, domain):
        """Get all active URLs for a specific domain under a specific site
        
        Args:
            site_id: Site ID
            domain: Domain name
            
        Returns:
            List of active URLs
        """
        with self.db_session() as session:
            sql = """
                SELECT id, url, url_md5, status FROM site_urls 
                WHERE site_id = :site_id 
                AND url LIKE :domain_pattern
                AND status IN ('pending', 'crawling', 'completed')
            """
            domain_pattern = f"%{domain}%"
            return session.execute(text(sql), {
                'site_id': site_id,
                'domain_pattern': domain_pattern
            }).fetchall()
    
    def mark_urls_as_removed(self, url_ids, reason="No longer found in source"):
        """Batch mark URLs as removed status
        
        Args:
            url_ids: List of URL IDs
            reason: Removal reason
        """
        if not url_ids:
            return 0
            
        with self.db_session() as session:
            removal_info = f"REMOVED: {reason} at {datetime.datetime.now(datetime.UTC).isoformat()}"
            
            sql = """
                UPDATE site_urls 
                SET status = 'removed', 
                    error_message = :removal_info,
                    updated_at = :updated_at
                WHERE id IN :url_ids
            """
            
            # Build IN clause
            placeholders = ','.join([':id' + str(i) for i in range(len(url_ids))])
            sql = sql.replace(':url_ids', f"({placeholders})")
            
            params = {
                'removal_info': removal_info,
                'updated_at': datetime.datetime.now(datetime.UTC)
            }
            
            for i, url_id in enumerate(url_ids):
                params[f'id{i}'] = url_id
            
            result = session.execute(text(sql), params)
            session.commit()
            
            logger.info(f"Marked {result.rowcount} URLs as removed: {reason}")
            return result.rowcount
    
    def cleanup_old_removed_urls(self, site_id=None, days_threshold=30):
        """Clean up URL records that have been marked as removed for a long time
        
        Args:
            site_id: Optional site ID
            days_threshold: Deletion threshold in days
        """
        with self.db_session() as session:
            base_sql = """
                DELETE FROM site_urls 
                WHERE status = 'removed' 
                AND updated_at < :threshold_date
            """
            
            params = {
                'threshold_date': datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=days_threshold)
            }
            
            if site_id:
                base_sql += " AND site_id = :site_id"
                params['site_id'] = site_id
            
            result = session.execute(text(base_sql), params)
            session.commit()
            
            cleaned_count = result.rowcount
            if cleaned_count > 0:
                site_info = f" for site {site_id}" if site_id else ""
                logger.info(f"Cleaned up {cleaned_count} old removed URLs{site_info}")
            
            return cleaned_count
    
    def discover_and_sync_links_from_content(self, html_content, base_url, site_id, allowed_domains=None):
        """Discover new links from page content and sync removed links
        
        Args:
            html_content: HTML content
            base_url: Page URL
            site_id: Site ID
            allowed_domains: List of allowed domains
        
        Returns:
            (new_links_count, removed_links_count): Number of new and removed links
        """
        try:
            # Extract all links from current page
            current_urls = set(self.extract_links_from_html(html_content, base_url, allowed_domains))
            
            # Get all active URLs for the same domain under this site
            if allowed_domains:
                domain = allowed_domains[0]
                existing_urls = self.get_urls_by_domain_from_site(site_id, domain)
            else:
                # If no domain restriction specified, get all active URLs
                with self.db_session() as session:
                    sql = """
                        SELECT id, url, url_md5, status FROM site_urls 
                        WHERE site_id = :site_id 
                        AND status IN ('pending', 'crawling', 'completed')
                    """
                    existing_urls = session.execute(text(sql), {'site_id': site_id}).fetchall()
            
            # Build mapping of existing URLs
            existing_urls_map = {url_record.url: url_record for url_record in existing_urls}
            existing_urls_set = set(existing_urls_map.keys())
            
            # Discover new URLs
            new_urls = current_urls - existing_urls_set
            new_links_count = 0
            for url in new_urls:
                try:
                    url_id = self.add_discovered_url(url, site_id, source='dynamic')
                    if url_id:
                        new_links_count += 1
                except Exception as e:
                    logger.warning(f"Failed to add discovered URL {url}: {str(e)}")
            
            # Find potentially removed URLs
            potentially_removed_urls = existing_urls_set - current_urls
            
            # Only mark dynamic source URLs as removed to be conservative
            removed_url_ids = []
            for url in potentially_removed_urls:
                url_record = existing_urls_map[url]
                if hasattr(url_record, 'source') and getattr(url_record, 'source', '') == 'dynamic':
                    removed_url_ids.append(url_record.id)
            
            removed_links_count = 0
            if removed_url_ids:
                removed_links_count = self.mark_urls_as_removed(
                    removed_url_ids, 
                    f"No longer found in page {base_url}"
                )
            
            if new_links_count > 0 or removed_links_count > 0:
                logger.info(f"Page {base_url}: +{new_links_count} new, -{removed_links_count} removed")
            
            return new_links_count, removed_links_count
            
        except Exception as e:
            logger.error(f"Error in discover_and_sync_links_from_content for {base_url}: {str(e)}")
            return 0, 0
    
    def discover_new_links_from_content(self, html_content, base_url, site_id, allowed_domains=None):
        """Discover new links from page content and add to database (backward compatibility)
        
        Args:
            html_content: HTML content
            base_url: Page URL
            site_id: Site ID
            allowed_domains: List of allowed domains
        
        Returns:
            Number of newly discovered links
        """
        new_count, _ = self.discover_and_sync_links_from_content(
            html_content, base_url, site_id, allowed_domains
        )
        return new_count
    
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
