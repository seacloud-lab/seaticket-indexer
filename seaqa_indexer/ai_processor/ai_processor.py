import logging
import datetime
import json

from seaqa_indexer.crawler.utils import url_to_filename
from seaqa_indexer.ai_processor.utils import generate_summary
from seaqa_indexer.utils.constants import ConnectionType
from seaqa_indexer.seadb_models.models import (
    WebCrawlTable,
    DiscourseTopicsTable,
    GithubIssuesTable,
    SeafileTable,
    ThreadTable
)
from seaqa_indexer.seadb_models.discourse_seadb_api import DiscourseSeaDBAPI
from seaqa_indexer.seadb_models.github_seadb_api import GitHubSeaDBAPI
from seaqa_indexer.seadb_models.seafile_seadb_api import SeafileSeaDBAPI
from seaqa_indexer.seadb_models.email_seadb_api import EmailSeaDBAPI
from seaqa_indexer.utils.constants import AIStatus

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
    ai_status = project_connection.ai_status
    if not ai_status:
        ai_status = {
            'last_ai_processing_status': AIStatus.PENDING,
            'last_ai_processing_count': 0,
        }
    else:
        ai_status = json.loads(ai_status)

    ai_status['last_ai_processing_status'] = AIStatus.PROCESSING
    seaqa_db.update_ai_status_and_time(connection_id, json.dumps(ai_status))

    try:
        if connection_type == ConnectionType.SITE.value:
            ai_processing_count = process_site_ai(seadb_api, storage, connection_id, project_uuid)
        elif connection_type == ConnectionType.DISCOURSE_FORUM.value:
            ai_processing_count = process_discourse_ai(seadb_api, connection_id, project_uuid)
        elif connection_type == ConnectionType.GITHUB_ISSUE.value:
            ai_processing_count = process_github_issue_ai(seadb_api, connection_id, project_uuid)
        elif connection_type == ConnectionType.SEAFILE.value:
            ai_processing_count = process_seafile_ai(seadb_api, connection_id, project_uuid)
        elif connection_type == ConnectionType.EMAIL.value:
            ai_processing_count = process_email_ai(seadb_api, connection_id, project_uuid)
        else:
            logger.warning('Unsupported connection type: %s for connection %s', connection_type, connection_id)
            return
        logger.info('AI processing completed for connection %s', connection_id)
        ai_status['last_ai_processing_status'] = AIStatus.COMPLETED
        ai_status['last_ai_processing_count'] = ai_processing_count
        last_ai_processing_time = datetime.datetime.now(datetime.UTC)
        seaqa_db.update_ai_status_and_time(connection_id, json.dumps(ai_status), last_ai_processing_time)
    except Exception as e:
        ai_status['last_ai_processing_status'] = AIStatus.FAILED
        last_ai_processing_time = datetime.datetime.now(datetime.UTC)
        seaqa_db.update_ai_status_and_time(connection_id, json.dumps(ai_status), last_ai_processing_time)
        raise e

def process_site_ai(seadb_api, storage, connection_id, project_uuid):
    page_size = 100
    updates = []
    ai_processing_count = 0
    table_name = WebCrawlTable.gen_table_name(connection_id)
    while True:
        sql = f"""
            SELECT _pk, url FROM `{table_name}`
            WHERE ai_processed_time IS NULL 
            OR ai_processed_time < sync_time
            ORDER BY _pk
            LIMIT {page_size}
        """
        result = seadb_api.query_rows(project_uuid, sql)
        rows = result.get('results', [])
        if not rows:
            break

        for row in rows:
            pk = row.get('_pk')
            url = row.get('url')
            filename = url_to_filename(url)
            file_json = storage.read_json(connection_id, project_uuid, filename)
            content = file_json.get('content', '')
            summary, vector = generate_summary(content, ConnectionType.SITE.value, project_uuid)
            updates.append({
                "pk": pk,
                "row": {
                    "ai_summary": summary,
                    "ai_summary_vector": vector,
                    "ai_processed_time": datetime.datetime.now(datetime.UTC).isoformat()

                }
            })
            ai_processing_count += 1
            if len(updates) >= BATCH_SIZE:
                seadb_api.update_rows(project_uuid, table_name, updates)
                updates = []
        if updates:
            seadb_api.update_rows(project_uuid, table_name, updates)
            updates = []
    return ai_processing_count


def process_discourse_ai(seadb_api, connection_id, project_uuid):
    page_size = 100
    updates = []
    table_name = DiscourseTopicsTable.gen_table_name(connection_id)
    discourse_seadb_api = DiscourseSeaDBAPI(project_uuid)

    ai_processing_count = 0
    while True:
        rows = discourse_seadb_api.get_need_ai_topics(connection_id, page_size)
        if not rows:
            break

        for row in rows:
            pk = row.get('_pk')
            topic_id = row.get('topic_id')
            title = row.get('title', '')
            result = discourse_seadb_api.get_topic_first_reply(connection_id, topic_id)
            first_content = result.get('content', '')
            full_content = f"Title: {title}\n\nBody: {first_content}\n"
            summary, vector = generate_summary(full_content, ConnectionType.DISCOURSE_FORUM.value, project_uuid)
            updates.append({
                "pk": pk,
                "row": {
                    "ai_summary": summary,
                    "ai_summary_vector": vector,
                    "ai_processed_time": datetime.datetime.now(datetime.UTC).isoformat()
                }
            })
            ai_processing_count += 1
            if len(updates) >= BATCH_SIZE:
                seadb_api.update_rows(project_uuid, table_name, updates)
                updates = []
        if updates:
            seadb_api.update_rows(project_uuid, table_name, updates)
    return ai_processing_count


def process_github_issue_ai(seadb_api, connection_id, project_uuid):
    page_size = 100
    updates = []
    table_name = GithubIssuesTable.gen_table_name(connection_id)
    github_seadb_api = GitHubSeaDBAPI(project_uuid)

    ai_processing_count = 0
    while True:
        rows = github_seadb_api.get_need_ai_issues_by_page(connection_id, page_size)
        if not rows:
            break
        for row in rows:
            pk = row.get('_pk')
            title = row.get('title', '')
            content = row.get('content', '')

            full_content = f"Title: {title}\n\nBody: {content}\n"
            summary, vector = generate_summary(full_content, ConnectionType.GITHUB_ISSUE.value, project_uuid)
            updates.append({
                "pk": pk,
                "row": {
                    "ai_summary": summary,
                    "ai_summary_vector": vector,
                    "ai_processed_time": datetime.datetime.now(datetime.UTC).isoformat()
                }
            })
            ai_processing_count += 1
            if len(updates) >= BATCH_SIZE:
                seadb_api.update_rows(project_uuid, table_name, updates)
                updates = []
        if updates:
            seadb_api.update_rows(project_uuid, table_name, updates)
    return ai_processing_count


def process_seafile_ai(seadb_api, connection_id, project_uuid):
    page_size = 100
    updates = []
    table_name = SeafileTable.gen_table_name(connection_id)
    seafile_api = SeafileSeaDBAPI(project_uuid)

    ai_processing_count = 0
    while True:
        rows = seafile_api.get_need_ai_files_by_page(connection_id, page_size)
        if not rows:
            break
        for row in rows:
            pk = row.get('_pk')
            title = row.get('title')
            content = row.get('content')
            summary = ''
            vector = []
            if content:
                full_content = f"Title: {title}\n\nContent: {content}\n"
                summary, vector = generate_summary(full_content, ConnectionType.SEAFILE.value, project_uuid)
            updates.append({
                "pk": pk,
                "row": {
                    "ai_summary": summary,
                    "ai_summary_vector": vector,
                    "ai_processed_time": datetime.datetime.now(datetime.UTC).isoformat()
                }
            })
            ai_processing_count += 1
            if len(updates) >= BATCH_SIZE:
                seadb_api.update_rows(project_uuid, table_name, updates)
                updates = []
        if updates:
            seadb_api.update_rows(project_uuid, table_name, updates)
    return ai_processing_count


def process_email_ai(seadb_api, connection_id, project_uuid):
    page_size = 100
    updates = []
    table_name = ThreadTable.gen_table_name(connection_id)
    email_seadb_api = EmailSeaDBAPI(project_uuid)

    ai_processing_count = 0
    while True:
        rows = email_seadb_api.get_need_ai_emails_by_page(connection_id, page_size)
        if not rows:
            break
        for row in rows:
            pk = row.get('_pk')
            title = row.get('title', '')
            result = email_seadb_api.get_first_email(connection_id, pk)
            content = result.get('content', '')

            full_content = f"Title: {title}\n\nBody: {content}\n"
            summary, vector = generate_summary(full_content, ConnectionType.EMAIL.value, project_uuid)
            updates.append({
                "pk": pk,
                "row": {
                    "ai_summary": summary,
                    "ai_summary_vector": vector,
                    "ai_processed_time": datetime.datetime.now(datetime.UTC).isoformat()
                }
            })
            ai_processing_count += 1
            if len(updates) >= BATCH_SIZE:
                seadb_api.update_rows(project_uuid, table_name, updates)
                updates = []
        if updates:
            seadb_api.update_rows(project_uuid, table_name, updates)
    return ai_processing_count
