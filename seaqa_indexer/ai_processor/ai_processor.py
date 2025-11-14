import logging
import datetime

from seaqa_indexer.crawler.utils import url_to_filename
from seaqa_indexer.ai_processor.utils import generate_summary
from seaqa_indexer.utils.constants import ConnectionType
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

BATCH_SIZE = 50

def generate_summaries_and_vectors(seadb_api, storage, seaqa_db, connection_id):
    project_connection = seaqa_db.get_project_connection_by_id(connection_id)
    if not project_connection:
        logger.warning('connection: %s not found', connection_id)
        return

    connection_is_active = project_connection.is_active
    if not connection_is_active:
        logger.warning('connection: %s is inactive', connection_id)
        return

    project_uuid = project_connection.project_uuid
    connection_type = project_connection.type

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

    logger.info('AI processing completed for connection %s', connection_id)

def process_site_ai(seadb_api, storage, connection_id, project_uuid):
    page_size = 100
    offset = 0
    updates = []
    table_name = WebCrawlTable.gen_table_name(connection_id)

    while True:
        sql = f"""
            SELECT _pk, url FROM `{table_name}`
            WHERE ai_processed_time IS NULL
            OR ai_processed_time < sync_time
            ORDER BY url
            LIMIT {page_size}
            OFFSET {offset}
        """
        result = seadb_api.query_rows(project_uuid, sql)
        offset += page_size
        rows = result.get('results', [])
        if not rows:
            break

        for row in rows:
            pk = row.get('_pk')
            url = row.get('url')
            filename = url_to_filename(url)
            file_json = storage.read_json(connection_id, project_uuid, filename)
            content = file_json.get('content', '')
            summary, vector = generate_summary(content, ConnectionType.SITE.value)
            updates.append({
                "pk": pk,
                "row": {
                    "ai_summary": summary,
                    "ai_summary_vector": vector,
                    "ai_processed_time": datetime.datetime.now(datetime.UTC).isoformat()
                }
            })
            if len(updates) >= BATCH_SIZE:
                seadb_api.update_rows(project_uuid, table_name, updates)
                updates = []
    if updates:
        seadb_api.update_rows(project_uuid, table_name, updates)
        logger.info('Final batch updated %d rows for site connection %s', len(updates), connection_id)


def process_discourse_ai(seadb_api, connection_id, project_uuid):
    page_size = 100
    offset = 0
    updates = []
    table_name = DiscourseTopicsTable.gen_table_name(connection_id)
    discourse_seadb_api = DiscourseSeaDBAPI(project_uuid)

    while True:
        rows = discourse_seadb_api.get_need_ai_topics(connection_id, page_size, offset)
        offset += page_size
        if not rows:
            break

        topic_ids = [row.get('topic_id') for row in rows]
        result = discourse_seadb_api.get_topics_first_reply(connection_id, topic_ids)
        topic_id_content_map = {}
        for topic_id, content in result.items():
            topic_id_content_map[topic_id] = content

        for row in rows:
            pk = row.get('_pk')
            topic_id = row.get('topic_id')
            title = row.get('title', '')

            first_content = topic_id_content_map.get(topic_id, '')
            full_content = f"Title: {title}\n\nBody: {first_content}\n"
            summary, vector = generate_summary(full_content, ConnectionType.DISCOURSE_FORUM.value)
            updates.append({
                "pk": pk,
                "row": {
                    "ai_summary": summary,
                    "ai_summary_vector": vector,
                    "ai_processed_time": datetime.datetime.now(datetime.UTC).isoformat()
                }
            })
            if len(updates) >= BATCH_SIZE:
                seadb_api.update_rows(project_uuid, table_name, updates)
                updates = []
    if updates:
        seadb_api.update_rows(project_uuid, table_name, updates)
        logger.info('Final batch updated %d rows for discourse connection %s', len(updates), connection_id)


def process_github_issue_ai(seadb_api, connection_id, project_uuid):
    page_size = 100
    offset = 0
    updates = []
    table_name = GithubIssuesTable.gen_table_name(connection_id)
    github_seadb_api = GitHubSeaDBAPI(project_uuid)

    while True:
        rows = github_seadb_api.get_need_ai_issues_by_page(connection_id, page_size, offset)
        if not rows:
            break
        offset += page_size
        for row in rows:
            pk = row.get('_pk')
            title = row.get('title', '')
            content = row.get('content', '')

            full_content = f"Title: {title}\n\nBody: {content}\n"
            summary, vector = generate_summary(full_content, ConnectionType.GITHUB_ISSUE.value)
            updates.append({
                "pk": pk,
                "row": {
                    "ai_summary": summary,
                    "ai_summary_vector": vector,
                    "ai_processed_time": datetime.datetime.now(datetime.UTC).isoformat()
                }
            })
            if len(updates) >= BATCH_SIZE:
                seadb_api.update_rows(project_uuid, table_name, updates)
                updates = []
    if updates:
        seadb_api.update_rows(project_uuid, table_name, updates)
        logger.info('Final batch updated %d rows for github issue connection %s', len(updates), connection_id)


def process_seafile_ai(seadb_api, connection_id, project_uuid):
    page_size = 100
    offset = 0
    updates = []
    table_name = SeafileTable.gen_table_name(connection_id)
    seafile_api = SeafileSeaDBAPI(project_uuid)

    while True:
        rows = seafile_api.get_need_ai_files_by_page(connection_id, page_size, offset)
        if not rows:
            break
        offset += page_size
        for row in rows:
            pk = row.get('_pk')
            title = row.get('title')
            content = row.get('content')
            summary = ''
            vector = []
            if content:
                full_content = f"Title: {title}\n\nContent: {content}\n"
                summary, vector = generate_summary(full_content, ConnectionType.SEAFILE.value)
            updates.append({
                "pk": pk,
                "row": {
                    "ai_summary": summary,
                    "ai_summary_vector": vector,
                    "ai_processed_time": datetime.datetime.now(datetime.UTC).isoformat()
                }
            })
            if len(updates) >= BATCH_SIZE:
                seadb_api.update_rows(project_uuid, table_name, updates)
                updates = []
    if updates:
        seadb_api.update_rows(project_uuid, table_name, updates)
        logger.info('Final batch updated %d rows for seafile connection %s', len(updates), connection_id)
