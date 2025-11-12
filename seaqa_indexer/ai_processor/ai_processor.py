import logging
import datetime

from seaqa_indexer.crawler.utils import url_to_filename
from seaqa_indexer.ai_processor.utils import generate_summary
from seaqa_indexer.utils.constants import LLM_INPUT_CHARACTERS_LIMIT, ConnectionType
from seaqa_indexer.seadb_models.models import (
    WebCrawlTable, 
    DiscourseTopicsTable, 
    GithubIssuesTable, 
    SeafileTable
)
from seaqa_indexer.seadb_models.discourse_seadb_api import DiscourseSeaDBAPI
from seaqa_indexer.seadb_models.github_seadb_api import GitHubSeaDBAPI
from seaqa_indexer.seadb_models.seafile_seadb_api import SeafileSeaDBAPI

logger = logging.getLogger(__name__)


def generate_summaries_and_vectors(seadb_api, storage, connection_id, project_uuid, connection_type):
    logger.info('Start AI processing for connection %s, type: %s', connection_id, connection_type)
    
    try:
        if connection_type == ConnectionType.SITE.value:
            process_site_ai(seadb_api, storage, connection_id, project_uuid)
        elif connection_type == ConnectionType.DISCOURSE_FORUM.value:
            process_discourse_ai(seadb_api, connection_id, project_uuid)
        elif connection_type == ConnectionType.GITHUB_ISSUE.value:
            process_github_issue_ai(seadb_api, connection_id, project_uuid)
        elif connection_type == ConnectionType.SEAFILE.value:
            process_seafile_ai(seadb_api, connection_id, project_uuid)
        else:
            logger.warning('Unsupported connection type: %s for connection %s', connection_type, connection_id)
            return
    except Exception as e:
        logger.exception('AI processing failed for connection %s, type %s: %s', connection_id, connection_type, e)
        raise
    
    logger.info('AI processing completed for connection %s', connection_id)


def _batch_update_rows(seadb_api, project_uuid, table_name, updates, batch_size=50):
    if len(updates) >= batch_size:
        try:
            seadb_api.update_rows(project_uuid, table_name, updates)
            logger.debug('Batch updated %d rows in table %s', len(updates), table_name)
            return []
        except Exception as e:
            logger.exception('Failed to batch update rows in table %s: %s', table_name, e)
            raise e
    return updates


def _generate_ai_content(params):
    try:
        summary, vector = generate_summary(params)
        return summary, vector
    except Exception as e:
        logger.exception('Failed to generate AI summary: %s', e)
        return None, None


def process_site_ai(seadb_api, storage, connection_id, project_uuid):
    page_size = 100
    offset = 0
    updates = []
    table_name = WebCrawlTable.gen_table_name(connection_id)
    params = {
        "username": "",
        "connection_type": ConnectionType.SITE.value,
        "include_vector": True
    }

    while True:
        sql = f"""SELECT * FROM `{table_name}` ORDER BY url LIMIT {page_size} OFFSET {offset}"""
        result = seadb_api.query_rows(project_uuid, sql)
        offset += page_size
        rows = result.get('results', [])
        if not rows:
            break
        
        for row in rows:
            ai_processed_time = row.get('ai_processed_time')
            sync_time = row.get('sync_time')
            pk = row.get('_pk')
            url = row.get('url')
            if ai_processed_time and sync_time < ai_processed_time:
                continue
            try:
                filename = url_to_filename(url)
                file_json = storage.read_json(connection_id, project_uuid, filename)
                content = file_json.get('content', '')
                params['content'] = content[:LLM_INPUT_CHARACTERS_LIMIT]
                summary, vector = _generate_ai_content(params)
                if summary:
                    updates.append({
                        "pk": pk,
                        "row": {
                            "ai_summary": summary,
                            "ai_summary_vector": vector,
                            "ai_processed_time": datetime.datetime.now(datetime.UTC).isoformat()
                        }
                    })
                    updates = _batch_update_rows(seadb_api, project_uuid, table_name, updates)
            except Exception as e:
                logger.exception('Error processing site URL %s: %s', url, e)
                continue
    if updates:
        try:
            seadb_api.update_rows(project_uuid, table_name, updates)
            logger.info('Final batch updated %d rows for site connection %s', len(updates), connection_id)
        except Exception as e:
            logger.exception('Failed to update final batch for connection %s', connection_id)


def process_discourse_ai(seadb_api, connection_id, project_uuid):
    page_size = 100
    offset = 0
    updates = []
    table_name = DiscourseTopicsTable.gen_table_name(connection_id)
    discourse_api = DiscourseSeaDBAPI(project_uuid)
    params = {
        "username": "",
        "connection_type": ConnectionType.DISCOURSE_FORUM.value,
        "include_vector": True
    }
    
    while True:
        sql = f"""SELECT * FROM `{table_name}` ORDER BY topic_id LIMIT {page_size} OFFSET {offset}"""
        result = seadb_api.query_rows(project_uuid, sql)
        offset += page_size
        rows = result.get('results', [])
        if not rows:
            break
        
        topic_ids = [row.get('topic_id') for row in rows]
        result = discourse_api.get_topics_first_reply(connection_id, topic_ids)
        topic_id_content_map = {}
        for topic_id, content in result.items():
            topic_id_content_map[topic_id] = content
        
        for row in rows:
            ai_processed_time = row.get('ai_processed_time')
            sync_time = row.get('sync_time')
            pk = row.get('_pk')
            topic_id = row.get('topic_id')
            title = row.get('title', '')
            if ai_processed_time and sync_time < ai_processed_time:
                continue
            
            first_content = topic_id_content_map.get(topic_id, '')
            try:
                full_content = f"Title: {title}\n" + "\n".join(first_content)
                params['content'] = full_content
                summary, vector = _generate_ai_content(params)
                if summary:
                    updates.append({
                        "pk": pk,
                        "row": {
                            "ai_summary": summary,
                            "ai_summary_vector": vector,
                            "ai_processed_time": datetime.datetime.now(datetime.UTC).isoformat()
                        }
                    })
                    updates = _batch_update_rows(seadb_api, project_uuid, table_name, updates)
            except Exception as e:
                logger.exception('Error processing discourse topic %s: %s', topic_id, e)
                continue
    if updates:
        try:
            seadb_api.update_rows(project_uuid, table_name, updates)
            logger.info('Final batch updated %d rows for discourse connection %s', len(updates), connection_id)
        except Exception as e:
            logger.exception('Failed to update final batch for connection %s', connection_id)


def process_github_issue_ai(seadb_api, connection_id, project_uuid):
    page_size = 100
    offset = 0
    updates = []
    table_name = GithubIssuesTable.gen_table_name(connection_id)
    github_api = GitHubSeaDBAPI(project_uuid)
    params = {
        "username": "",
        "connection_type": ConnectionType.GITHUB_ISSUE.value,
        "include_vector": True
    }
    
    while True:
        rows = github_api.get_all_issues_by_page(connection_id, page_size, offset)
        if not rows:
            break
        offset += page_size
        for row in rows:
            pk = row.get('_pk')
            issue_id = row.get('issue_id')
            title = row.get('title', '')
            content = row.get('content', '')
            sync_time = row.get('sync_time')
            ai_processed_time = row.get('ai_processed_time')
            
            if ai_processed_time and sync_time < ai_processed_time:
                continue
            
            try:
                full_content = f"Title: {title}\n\nBody: {content}\n\n"
                params['content'] = full_content
                summary, vector = _generate_ai_content(params)
                if summary:
                    updates.append({
                        "pk": pk,
                        "row": {
                            "ai_summary": summary,
                            "ai_summary_vector": vector,
                            "ai_processed_time": datetime.datetime.now(datetime.UTC).isoformat()
                        }
                    })
                    updates = _batch_update_rows(seadb_api, project_uuid, table_name, updates)
            except Exception as e:
                logger.exception('Error processing github issue %s: %s', issue_id, e)
                continue
    if updates:
        try:
            seadb_api.update_rows(project_uuid, table_name, updates)
            logger.info('Final batch updated %d rows for github issue connection %s', len(updates), connection_id)
        except Exception as e:
            logger.exception('Failed to update final batch for connection %s', connection_id)


def process_seafile_ai(seadb_api, connection_id, project_uuid):
    page_size = 100
    offset = 0
    updates = []
    table_name = SeafileTable.gen_table_name(connection_id)
    seafile_api = SeafileSeaDBAPI(project_uuid)
    params = {
        "username": "",
        "connection_type": ConnectionType.SEAFILE.value,
        "include_vector": True
    }
    
    while True:
        rows = seafile_api.get_all_files_by_page(connection_id, page_size, offset)
        if not rows:
            break
        offset += page_size
        for row in rows:
            pk = row.get('_pk')
            title = row.get('title')
            content = row.get('content')
            sync_time = row.get('sync_time')
            ai_processed_time = row.get('ai_processed_time')
            
            if ai_processed_time and sync_time < ai_processed_time:
                continue
            if not content:
                updates.append({
                        "pk": pk,
                        "row": {
                            "ai_summary": "",
                            "ai_summary_vector": [],
                            "ai_processed_time": datetime.datetime.now(datetime.UTC).isoformat()
                        }
                    })
                updates = _batch_update_rows(seadb_api, project_uuid, table_name, updates)
                continue
            try:
                full_content = f"Title: {title}\nContent: {content}\n"
                params['content'] = full_content
                summary, vector = _generate_ai_content(params)
                if summary:
                    updates.append({
                        "pk": pk,
                        "row": {
                            "ai_summary": summary,
                            "ai_summary_vector": vector,
                            "ai_processed_time": datetime.datetime.now(datetime.UTC).isoformat()
                        }
                    })
                    updates = _batch_update_rows(seadb_api, project_uuid, table_name, updates)
            except Exception as e:
                logger.exception('Error processing seafile file %s: %s', title, e)
                continue
    if updates:
        try:
            seadb_api.update_rows(project_uuid, table_name, updates)
            logger.info('Final batch updated %d rows for seafile connection %s', len(updates), connection_id)
        except Exception as e:
            logger.exception('Failed to update final batch for connection %s', connection_id)