import logging
import jwt
import json
from functools import wraps
from flask import request, jsonify, Flask

from seaqa_indexer import config
from seaqa_indexer.syncer.sync_manager import ForumNotFoundException, \
    ForumInActiveException
from seaqa_indexer.utils.constants import ConnectionType
from seaqa_indexer.crawler.storage import get_storage


application = Flask(__name__)
logger = logging.getLogger(__name__)


def check_auth_token(req):
    """Verify the JWT token in the request"""
    auth = req.headers.get('Authorization', '').split()
    if not auth or auth[0].lower() not in ('token', 'bearer') or len(auth) != 2:
        return False

    token = auth[1]
    if not token:
        return False

    private_key = config.JWT_PRIVATE_KEY
    try:
        jwt.decode(token, private_key, algorithms=['HS256'])
    except (jwt.ExpiredSignatureError, jwt.InvalidSignatureError) as e:
        logger.warning(f"Token verification failed: {str(e)}")
        return False

    return True


def auth_required(func):
    """Permission Verification Decorator"""
    @wraps(func)
    def decorated(*args, **kwargs):
        is_valid = check_auth_token(request)
        if not is_valid:
            return {'error_msg': 'Permission denied'}, 403
        return func(*args, **kwargs)
    return decorated


@application.route('/add-connection-sync-task', methods=['GET'])
@auth_required
def add_connection_sync_task():
    connection_id = request.args.get('connection_id')
    connection_type = request.args.get('type')

    if connection_type == ConnectionType.SITE.value:
        application.app.web_crawl_manager.add_crawl_site_task(connection_id)
    elif connection_type in (
        ConnectionType.GITHUB_ISSUE.value,
        ConnectionType.DISCOURSE_FORUM.value,
        ConnectionType.SEAFILE.value,
        ConnectionType.EMAIL.value,
        ConnectionType.NOTION.value,
        ConnectionType.GENERAL_TASK.value,
        ConnectionType.LINEAR.value,
        ConnectionType.CONFLUENCE.value,
        ConnectionType.DISCORD.value,
        ConnectionType.JIRA_ISSUE.value,
        ConnectionType.FIREBASE_CRASH.value,
    ):
        application.app.sync_manager.check_and_sync(connection_id)
    else:
        return jsonify({'success': False, 'error_msg': f'Unsupported type: {connection_type}'}), 400

    return jsonify({'success': True}), 200


@application.route('/cleanup-deleted-project', methods=['POST'])
@auth_required
def cleanup_deleted_project():
    data = request.get_json(force=True)
    project_uuid = data.get('project_uuid')

    if not project_uuid:
        return jsonify({'success': False, 'error_msg': 'Missing project_uuid parameter'}), 400

    try:
        # delete project index
        application.app.index_manager.delete_index_by_project(project_uuid)

        storage = get_storage()
        # delete crawled file folder
        storage.delete_project_crawl_data(project_uuid)
        # delete project attachment
        storage.delete_project_attachment(project_uuid)
    except Exception as e:
        logger.exception('cleanup deleted project failed: %s', e)
        return jsonify({'success': False, 'error_msg': 'Internal server error'}), 500

    return jsonify({'success': True}), 200


@application.route('/cleanup-deleted-connection', methods=['POST'])
@auth_required
def cleanup_deleted_connection():
    data = request.get_json(force=True)
    project_uuid = data.get('project_uuid')
    connection_id = data.get('connection_id')
    connection_type = data.get('connection_type')

    if not project_uuid:
        return jsonify({'success': False, 'error_msg': 'Missing project_uuid parameter'}), 400
    if not connection_id:
        return jsonify({'success': False, 'error_msg': 'Missing connection_id parameter'}), 400
    if not connection_type:
        return jsonify({'success': False, 'error_msg': 'Missing connection_type parameter'}), 400
    if not ConnectionType.is_valid(connection_type):
        return jsonify({'success': False, 'error_msg': 'Unsupported connection type'}), 400

    try:
        application.app.index_manager.delete_index_by_connection(connection_id, connection_type)
        if connection_type in (ConnectionType.SITE.value, ConnectionType.EMAIL.value):
            storage = get_storage()
            storage.delete_connection_crawl_data(connection_id, project_uuid)
    except Exception as e:
        logger.exception('cleanup deleted connection failed: %s', e)
        return jsonify({'success': False, 'error_msg': 'Internal server error'}), 500

    return jsonify({'success': True}), 200


@application.route('/storage-usage/project', methods=['POST'])
@auth_required
def project_storage_usage():
    try:
        data = request.get_json(force=True) or {}
    except Exception:
        return jsonify({'success': False, 'error_msg': 'Bad request'}), 400
    project_uuid = data.get('project_uuid')
    if not project_uuid:
        return jsonify({'success': False, 'error_msg': 'Missing project_uuid parameter'}), 400

    try:
        usage = get_storage().get_project_storage_usage(project_uuid)
    except Exception:
        logger.exception('Project storage usage failed for project %s', project_uuid)
        return jsonify({'success': False, 'error_msg': 'Internal server error'}), 500

    return jsonify({
        'success': True,
        'usage': {
            'file_size': usage['file_size'],
            'file_object_count': usage['file_object_count'],
            'crawl_data_size': usage['crawl_data_size'],
            'crawl_object_count': usage['crawl_object_count'],
        },
    }), 200

@application.route('/manual-sync-connection', methods=['POST'])
@auth_required
def manual_sync_connection():
    data = request.get_json(force=True)
    connection_id = data.get('connection_id')
    connection_type = data.get('connection_type')

    if not connection_id:
        return jsonify({'success': False, 'error_msg': 'Missing connection_id parameter'}), 400

    # Enqueue sync task
    try:
        if connection_type == ConnectionType.SITE.value:
            application.app.web_crawl_manager.add_crawl_site_task(connection_id)
        elif connection_type in (
            ConnectionType.GITHUB_ISSUE.value,
            ConnectionType.DISCOURSE_FORUM.value,
            ConnectionType.SEAFILE.value,
            ConnectionType.EMAIL.value,
            ConnectionType.NOTION.value,
            ConnectionType.GENERAL_TASK.value,
            ConnectionType.LINEAR.value,
            ConnectionType.CONFLUENCE.value,
            ConnectionType.DISCORD.value,
            ConnectionType.JIRA_ISSUE.value,
            ConnectionType.FIREBASE_CRASH.value,
        ):
            application.app.sync_manager.check_and_sync(connection_id)
        else:
            return jsonify({'success': False, 'error_msg': 'Unsupported connection type'}), 400
    except Exception as e:
        logger.exception('enqueue manual sync failed: %s', e)
        return jsonify({'success': False, 'error_msg': 'Internal server error'}), 500

    return jsonify({'success': True}), 200


@application.route('/webhook/github/', methods=['POST'])
@auth_required
def github_webhook():
    try:
        data = request.get_json(force=True)
    except Exception as e:
        logger.exception(e)
        return {'error_msg': 'Bad request.'}, 400

    installation_id = data.get('installation_id')
    update_data = data.get('update_data')

    if not installation_id:
        return jsonify({'error_msg': 'Missing installation_id parameter'}), 400
    action = data.get('action')
    event = data.get('event')
    repository_html_url = data.get('repository_html_url')

    issue_id = ''
    if event == 'issues':
        try:
            issue_id = update_data['id']
        except KeyError as e:
            return jsonify({'error_msg': 'Missing expected field in webhook payload'}), 400
    elif event == 'issue_comment':
        try:
            issue_data = update_data.get('issue')
            issue_id = issue_data['id']
        except KeyError as e:
            return jsonify({'error_msg': 'Missing expected field in webhook payload'}), 400

    try:
        application.app.sync_manager.update_github_issues_by_webhook(installation_id, update_data, issue_id, action, event, repository_html_url)
    except Exception as e:
        logger.error(e)
        return jsonify({'error_msg': 'Internal Server Error'}), 500

    return jsonify({'success': True}), 200


@application.route('/webhook/confluence/', methods=['POST'])
@auth_required
def confluence_webhook():
    try:
        data = request.get_json(force=True)
    except Exception as e:
        logger.exception(e)
        return {'error_msg': 'Bad request.'}, 400

    workspace_id = data.get('workspace_id')
    if not workspace_id:
        return jsonify({'error_msg': 'Missing workspace_id parameter'}), 400

    try:
        application.app.sync_manager.handle_confluence_webhook(workspace_id)
    except Exception as e:
        logger.error('Confluence webhook error: %s', e)
        return jsonify({'error_msg': 'Internal Server Error'}), 500

    return jsonify({'success': True}), 200


@application.route('/search/keyword', methods=['POST'])
@auth_required
def keyword_search():
    try:
        data = json.loads(request.data)
    except Exception as e:
        logger.exception(e)
        return {'error_msg': 'Bad request.'}, 400

    query = data.get('query').strip()
    project_uuid = data.get('project_uuid')
    connection_ids = data.get('connection_ids')
    time_from = data.get('time_from')
    time_to = data.get('time_to')
    extra_sources = data.get('extra_sources', [])

    if not query:
        return {'error_msg': 'query invalid.'}, 400

    if not project_uuid:
        return {'error_msg': 'project_uuid invalid.'}, 400

    try:
        count = int(data.get('count'))
    except:
        count = 20

    results = application.app.index_manager.keyword_search(project_uuid, connection_ids, extra_sources, time_from, time_to, query, start=0, size=count)

    return {'results': results}, 200

@application.route('/search/vector', methods=['POST'])
@auth_required
def vector_search():
    try:
        data = json.loads(request.data)
    except Exception as e:
        logger.exception(e)
        return {'error_msg': 'Bad request.'}, 400

    query_vector = data.get('query_vector')
    project_uuid = data.get('project_uuid')
    connection_ids = data.get('connection_ids')
    extra_sources = data.get('extra_sources', [])
    score_threshold = data.get('score_threshold')

    if not query_vector:
        return {'error_msg': 'query_vector invalid.'}, 400

    if not project_uuid:
        return {'error_msg': 'project_uuid invalid.'}, 400

    if not isinstance(extra_sources, (list, tuple)):
        return {'error_msg': 'extra_sources invalid.'}, 400

    try:
        score_threshold = int(score_threshold)
    except:
        score_threshold = 0.5

    try:
        count = int(data.get('count'))
    except:
        count = 10
    results = application.app.index_manager.vector_search(project_uuid, connection_ids, extra_sources, query_vector, top_k=count, score_threshold=score_threshold)

    return {'results': results}, 200

@application.route('/search/vector-from-text', methods=['POST'])
@auth_required
def vector_search_by_text():
    try:
        data = json.loads(request.data)
    except Exception as e:
        logger.exception(e)
        return {'error_msg': 'Bad request.'}, 400

    query = data.get('query').strip()
    project_uuid = data.get('project_uuid')
    connection_ids = data.get('connection_ids')
    time_from = data.get('time_from')
    time_to = data.get('time_to')
    extra_sources = data.get('extra_sources', [])

    if not query:
        return {'error_msg': 'query invalid.'}, 400

    if not project_uuid:
        return {'error_msg': 'project_uuid invalid.'}, 400

    try:
        count = int(data.get('count'))
    except:
        count = 20
    results = application.app.index_manager.vector_search_by_text(project_uuid, connection_ids, extra_sources, query, time_from, time_to, count)

    return {'results': results}, 200


@application.route('/webhook/discourse/', methods=['POST'])
@auth_required
def discourse_webhook():
    """Handle Discourse webhook callback"""
    connection_id = request.args.get('connection_id')

    if not connection_id:
        return jsonify({'error': 'Missing connection_id'}), 400

    data = request.get_json(force=True)
    event_type = request.headers.get('X-Discourse-Event')

    if not data:
        return jsonify({'error': 'No payload data'}), 400

    if not event_type:
        return jsonify({'error': 'event_type invalid'}), 400

    try:
        application.app.sync_manager.handle_discourse_webhook(connection_id, data, event_type)
    except ForumNotFoundException:
        return {'error': 'Forum not found'}, 404
    except ForumInActiveException:
        logger.warning('Connection: %s is inactive', connection_id)
        return {'warning': 'connection is inactive,request ignored'}, 200
    except Exception as e:
        logger.error(f"Discourse webhook error: {e}")
        return {'error': 'Internal Server Error'}, 500

    return {'msg': 'Discourse event processed'}, 200
