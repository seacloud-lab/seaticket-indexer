import logging
import jwt
import json
import datetime
from functools import wraps
from flask import request, jsonify, Flask
from zoneinfo import ZoneInfo

from seaqa_indexer import config
from seaqa_indexer.syncer.github_issues_syncer.github_issues_sync_manager import SignatureInvalidException
from seaqa_indexer.syncer.discourse_syncer.discourse_sync_manager import ForumNotFoundException
from seaqa_indexer.utils.constants import ConnectionType, CrawlStatus, IndexStatus


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
    try:
        data = request.get_json(force=True)
        connection_id = data.get('connection_id')
        connection_type = data.get('connection_type')
        
        if not connection_id:
            return jsonify({'success': False, 'error_msg': 'Missing connection_id parameter'}), 400

        connection = application.index_manager.get_project_connection_by_id(connection_id)
        if not connection:
            return jsonify({'success': False, 'error_msg': 'connection not found'}), 404

        try:
            connection_status = json.loads(connection.status) if connection.status else {}
        except Exception:
            connection_status = {}

        if connection_status.get('last_sync_status') == CrawlStatus.CRAWLING.value:
            return jsonify({'success': False, 'error_msg': 'Connection is currently syncing'}), 400
        elif connection_status.get('last_sync_status') == CrawlStatus.PENDING.value:
            return jsonify({'success': False, 'error_msg': 'Connection is currently pending'}), 400
        elif connection_status.get('last_index_status') == IndexStatus.INDEXING.value:
            return jsonify({'success': False, 'error_msg': 'Connection is currently indexing'}), 400
        elif connection_status.get('last_index_status') == IndexStatus.PENDING.value:
            return jsonify({'success': False, 'error_msg': 'Connection is currently pending'}), 400

        if connection.indexed_at:
            now = datetime.datetime.now(datetime.timezone.utc)
            indexed_at_utc = connection.indexed_at.replace(tzinfo=datetime.timezone.utc)
            time_diff = now - indexed_at_utc
            cooldown_seconds = 24 * 60 * 60
            if time_diff.total_seconds() < cooldown_seconds:
                next_sync_time = indexed_at_utc + datetime.timedelta(seconds=cooldown_seconds)
                local_tz = ZoneInfo(config.TIME_ZONE)
                local_time_aware = next_sync_time.replace(tzinfo=ZoneInfo('UTC')).astimezone(local_tz)
                next_sync_display = local_time_aware.strftime('%Y-%m-%d %H:%M:%S')
                return jsonify({'success': False, 'error_msg': f'Manual sync too frequent. next time: {next_sync_display}'}), 400

        def _enqueue_wrapper(func, *args, **kwargs):
            try:
                func(*args, **kwargs)
                return True, 'Manual sync task enqueued'
            except Exception as e:
                logger.exception('enqueue manual sync failed: %s', e)
                return False, f'Failed to enqueue manual sync: {str(e)}'

        if connection_type == ConnectionType.SITE.value:
            success, message = _enqueue_wrapper(application.web_crawl_manager.add_crawl_site_task, connection_id)
        elif connection_type == ConnectionType.GITHUB_ISSUE.value:
            success, message = _enqueue_wrapper(application.github_issues_sync_manager.add_github_issues_sync_task, connection_id)
        elif connection_type == ConnectionType.DISCOURSE_FORUM.value:
            success, message = _enqueue_wrapper(application.discourse_sync_manager.add_discourse_sync_task, connection_id)
        elif connection_type == ConnectionType.SEAFILE.value:
            success, message = _enqueue_wrapper(application.index_manager.add_connection_index_task, connection_id)
        
        if success:
            return jsonify({'success': True, 'message': message}), 200
        else:
            return jsonify({'success': False, 'error_msg': message}), 400
            
    except Exception as e:
        logger.exception('Manual sync connection API error: %s', e)
        return jsonify({'success': False, 'error_msg': 'Internal server error'}), 500


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
def github_webhook():
    connection_id = request.args.get('connection_id')

    if not connection_id:
        return jsonify({'error': 'Missing connection_id'}), 400

    payload = request.get_json(force=True)

    try:
        issue_id = payload['issue']['id']
    except KeyError as e:
        return jsonify({'error': 'Missing expected field in webhook payload'}), 400

    action = payload.get('action')

    event = request.headers.get('X-GitHub-Event')
    if event != 'issues':
        return jsonify({'msg': f'Ignored event: {event}'}), 200

    issues_data = payload.get('issue')
    if not issues_data:
        return jsonify({'error': 'No issue payload'}), 400

    signature = request.headers.get('X-Hub-Signature-256')
    msg = request.data

    project_connection = application.app.github_issues_sync_manager.get_project_connection_by_id(connection_id)
    if not project_connection:
        return jsonify({'error': 'connection not found'}), 404

    try:
        application.app.github_issues_sync_manager.update_github_issues_by_webhook(connection_id, issues_data, issue_id, action, signature, msg)
    except SignatureInvalidException:
        return jsonify({'error': 'Invalid signature'}), 403
    except Exception as e:
        logger.error(e)
        return jsonify({'error': 'Internal Server Error'}), 500

    return jsonify({'msg': 'Issue event processed'}), 200


@application.route('/search', methods=['POST'])
@auth_required
def search():
    is_valid = check_auth_token(request)
    if not is_valid:
        return {'error_msg': 'Permission denied'}, 403

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
    except Exception as e:
        logger.error(f"Discourse webhook error: {e}")
        return {'error': 'Internal Server Error'}, 500

    return {'msg': 'Discourse event processed'}, 200
