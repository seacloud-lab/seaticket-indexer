import logging
import jwt
import json
from functools import wraps
from flask import request, jsonify, Flask

from seaqa_indexer import config
from seaqa_indexer.syncer.sync_manager import ForumNotFoundException, \
    ForumInActiveException
from seaqa_indexer.utils.constants import ConnectionType
from seaqa_indexer.handlers import get_handler
from seaqa_indexer.seadb_models.models import KnowledgeBaseTable, TicketsTable
from seaqa_indexer.utils.seadb_api import SeaDBAPI


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
    elif connection_type in (ConnectionType.GITHUB_ISSUE.value, ConnectionType.DISCOURSE_FORUM.value,
                       ConnectionType.SEAFILE.value, ConnectionType.EMAIL.value):
        application.app.sync_manager.check_and_sync(connection_id)
    else:
        return jsonify({'success': False, 'error_msg': f'Unsupported type: {connection_type}'}), 400

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
        elif connection_type in (ConnectionType.GITHUB_ISSUE.value, ConnectionType.DISCOURSE_FORUM.value,
                                 ConnectionType.SEAFILE.value, ConnectionType.EMAIL.value):
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


def _remove_deleted_search_results(project_uuid, results):
    conn_ids_pks = {}
    conn_ids_type = {}
    kb_ids = []
    tk_ids = []

    for result in results:
        if result.get('type') == 'knowledge_base':
            kb_ids.append(int(result.get('_id')))
        elif result.get('type') == 'ticket':
            tk_ids.append(int(result.get('_id')))
        else:
            connection_id = int(result.get('connection_id'))
            if connection_id not in conn_ids_pks:
                conn_ids_pks[connection_id] = []
                conn_ids_type[connection_id] = result.get('type')
            conn_ids_pks[connection_id].append(int(result.get('_id')))

    seadb_api = SeaDBAPI()

    def get_valid_pks(table_name, pks):
        sql = f"SELECT `_pk`, `deleted` FROM `{table_name}` WHERE `_pk` IN ({','.join([str(i) for i in pks])})"
        res = seadb_api.query_rows(project_uuid, sql, convert_keys=True)
        rows = res.get('results', [])
        return {row.get('_pk') for row in rows if not row.get('deleted')}

    valid_conn_ids_pks = {}
    for connection_id, pks in conn_ids_pks.items():
        handler = get_handler(conn_ids_type[connection_id])
        if not handler:
            logger.warning(f'Handler not found for connection {connection_id}')
            continue
        valid_conn_ids_pks[connection_id] = get_valid_pks(handler.gen_table_name(connection_id), pks)

    valid_kb_ids = get_valid_pks(KnowledgeBaseTable.gen_table_name(), kb_ids) if kb_ids else []
    valid_tk_ids = get_valid_pks(TicketsTable.gen_table_name(), tk_ids) if tk_ids else []

    new_results = []
    for result in results:
        if result.get('type') == 'ticket':
            if int(result.get('_id')) in valid_tk_ids:
                new_results.append(result)
        elif result.get('type') == 'knowledge_base':
            if int(result.get('_id')) in valid_kb_ids:
                new_results.append(result)
        elif int(result.get('_id')) in valid_conn_ids_pks.get(int(result.get('connection_id')), []):
             new_results.append(result)

    return new_results

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
    connection_ids = data.get('connection_ids')
    time_from = data.get('time_from')
    time_to = data.get('time_to')
    search_type = data.get('search_type', 'normal_search')
    username = data.get('username', '')
    extra_sources = data.get('extra_sources', [])
    caller = request.args.to_dict().get('from')

    if not query:
        return {'error_msg': 'query invalid.'}, 400

    if not project_uuid:
        return {'error_msg': 'project_uuid invalid.'}, 400

    try:
        count = int(data.get('count'))
    except:
        count = 20
    if search_type == 'semantic_search':
        results = application.app.index_manager.semantic_search(project_uuid, connection_ids, extra_sources, query, username, time_from, time_to, count, caller=caller)
    else:
        results = application.app.index_manager.search(project_uuid, connection_ids, extra_sources, time_from, time_to, query, start=0, size=count, caller=caller)

    if caller != 'seaqa-ai':
        results = _remove_deleted_search_results(project_uuid, results)

    return {'results': results}, 200


@application.route('/vector_search', methods=['POST'])
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
    caller = request.args.to_dict().get('from')

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
    results = application.app.index_manager.vector_search(project_uuid, connection_ids, extra_sources, query_vector, top_k=count, score_threshold=score_threshold, caller=caller)
    
    if caller != 'seaqa-ai':
        results = _remove_deleted_search_results(project_uuid, results)

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
