import logging
import jwt
import json
import hmac
import hashlib
from functools import wraps
from flask import request, jsonify, Flask

from seaqa_indexer import config

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


def verify_signature(request, github_secret):
    signature = request.headers.get('X-Hub-Signature-256')
    if not signature:
        return False
    sha_name, signature = signature.split('=')

    if sha_name != 'sha256':
        return False
    mac = hmac.new(github_secret.encode(), msg=request.data, digestmod=hashlib.sha256)

    return hmac.compare_digest(mac.hexdigest(), signature)


def auth_required(func):
    """Permission Verification Decorator"""
    @wraps(func)
    def decorated(*args, **kwargs):
        is_valid = check_auth_token(request)
        if not is_valid:
            return {'error_msg': 'Permission denied'}, 403
        return func(*args, **kwargs)
    return decorated


@application.route('/add-init-crawl-site-task', methods=['GET'])
@auth_required
def add_init_crawl_site_task():
    site_id = request.args.get('site_id')
    application.web_crawl_manager.add_crawl_site_task(site_id)

    return jsonify({'success': True}), 200


@application.route('/add-index-seafile-task', methods=['GET'])
@auth_required
def add_index_seafile_task():
    connection_id = request.args.get('connection_id')
    application.index_manager.add_connection_index_task(connection_id)

    return jsonify({'success': True}), 200


@application.route('/add-github-issues-index-task', methods=['GET'])
@auth_required
def add_github_issues_index_task():
    issues_sync_id = request.args.get('issue_index_id')
    application.github_issues_sync_manager.add_github_issues_sync_task(issues_sync_id)
    return jsonify({'success': True}), 200


@application.route('/webhook/github', methods=['POST'])
def github_webhook():
    raw_data = request.get_data()
    payload = json.loads(raw_data)

    try:
        repo_owner = payload['repository']['owner']['login']
        repo_name = payload['repository']['name']
        issue_number = payload['issue']['number']
    except KeyError as e:
        raise ValueError(f"Missing expected field in webhook payload: {e}")

    issues_sync_id = application.github_issues_sync_manager.get_issues_sync_id_by_issue_info(repo_name, repo_owner)
    secret = application.github_issues_sync_manager.get_webhook_secret(issues_sync_id)
    if not verify_signature(request, secret):
        return jsonify({'error': 'Invalid signature'}), 403

    event = request.headers.get('X-GitHub-Event')
    if event != 'issues':
        return jsonify({'msg': f'Ignored event: {event}'}), 200

    action = payload.get('action')
    logger.info(f"Received webhook for issue #{issue_number}, action: {action}")

    issues_data = payload.get('issue')
    if not issues_data:
        return jsonify({'error': 'No issue payload'}), 400

    application.github_issues_sync_manager.update_github_issues_by_webhook(issues_sync_id, issues_data, repo_owner, repo_name)

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

    if not query:
        return {'error_msg': 'query invalid.'}, 400

    if not project_uuid:
        return {'error_msg': 'project_uuid invalid.'}, 400

    try:
        count = int(data.get('count'))
    except:
        count = 20

    results = application.index_manager.search(project_uuid, connection_types, query, start=0, size=count)

    return {'results': results}, 200
