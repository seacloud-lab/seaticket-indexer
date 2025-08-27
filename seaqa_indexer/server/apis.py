import logging
import jwt
import json
from functools import wraps
from flask import request, jsonify, Flask

from seaqa_indexer import config
from seaqa_indexer.syncer.discourse_syncer.discourse_sync_manager import ForumNotFoundException, \
    ForumInActiveException
from seaqa_indexer.utils.constants import ConnectionType


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


@application.route('/add-init-crawl-task', methods=['GET'])
@auth_required
def add_init_crawl_task():
    connection_id = request.args.get('connection_id')
    task_type = request.args.get('type')
    if task_type == ConnectionType.SITE.value:
        application.app.web_crawl_manager.add_crawl_site_task(connection_id)
    elif task_type == ConnectionType.DISCOURSE_FORUM.value:
        application.app.discourse_sync_manager.add_discourse_sync_task(connection_id)
    else:
        return jsonify({'success': False, 'error_msg': f'Unsupported type: {task_type}'}), 400

    return jsonify({'success': True}), 200


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
        elif connection_type == ConnectionType.GITHUB_ISSUE.value:
            application.app.github_issues_sync_manager.add_github_issues_sync_task(connection_id)
        elif connection_type == ConnectionType.DISCOURSE_FORUM.value:
            application.app.discourse_sync_manager.add_discourse_sync_task(connection_id)
        elif connection_type == ConnectionType.SEAFILE.value:
            application.app.index_manager.add_connection_index_task(connection_id)
        else:
            return jsonify({'success': False, 'error_msg': 'Unsupported connection type'}), 400
    except Exception as e:
        logger.exception('enqueue manual sync failed: %s', e)
        return jsonify({'success': False, 'error_msg': 'Internal server error'}), 500

    return jsonify({'success': True}), 200


@application.route('/add-index-seafile-task', methods=['GET'])
@auth_required
def add_index_seafile_task():
    connection_id = request.args.get('connection_id')
    application.app.index_manager.add_connection_index_task(connection_id)

    return jsonify({'success': True}), 200


@application.route('/add-github-issues-index-task', methods=['GET'])
@auth_required
def add_github_issues_index_task():
    connection_id = request.args.get('connection_id')
    application.app.github_issues_sync_manager.add_github_issues_sync_task(connection_id)
    return jsonify({'success': True}), 200


@application.route('/webhook/github/', methods=['POST'])
@auth_required
def github_webhook():
    try:
        data = request.get_json(force=True)
    except Exception as e:
        logger.exception(e)
        return {'error_msg': 'Bad request.'}, 400

    connection_id = data.get('connection_id')
    issue_data = data.get('issue_data')

    if not connection_id:
        return jsonify({'error_msg': 'Missing connection_id parameter'}), 400
    action = request.args.get('action')

    try:
        issue_id = issue_data['id']
    except KeyError as e:
        return jsonify({'error_msg': 'Missing expected field in webhook payload'}), 400

    try:
        application.app.github_issues_sync_manager.update_github_issues_by_webhook(connection_id, issue_data, issue_id, action)
    except Exception as e:
        logger.error(e)
        return jsonify({'error_msg': 'Internal Server Error'}), 500

    return jsonify({'success': True}), 200


@application.route('/search', methods=['POST'])
@auth_required
def search():
    try:
        data = json.loads(request.data)
    except Exception as e:
        logger.exception(e)
        return {'error_msg': 'Bad request.'}, 400

    query = data.get('query').strip()
    project_uuid = data.get('project_uuid')
    connection_types = data.get('connection_types')
    connection_ids = data.get('connection_ids')

    if not query:
        return {'error_msg': 'query invalid.'}, 400

    if not project_uuid:
        return {'error_msg': 'project_uuid invalid.'}, 400

    try:
        count = int(data.get('count'))
    except:
        count = 20

    results = application.app.index_manager.search(project_uuid, connection_types, connection_ids, query, start=0, size=count)

    return {'results': results}, 200


@application.route('/discourse/webhook/', methods=['POST'])
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
        application.app.discourse_sync_manager.handle_webhook(connection_id, data, event_type)
    except ForumNotFoundException:
        return {'error': 'Forum not found'}, 404
    except ForumInActiveException:
        logger.warning('Connection: %s is inactive', connection_id)
        return {'warning': 'connection is inactive,request ignored'}, 200
    except Exception as e:
        logger.error(f"Discourse webhook error: {e}")
        return {'error': 'Internal Server Error'}, 500

    return {'msg': 'Discourse event processed'}, 200
