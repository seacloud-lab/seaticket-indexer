import logging
import jwt
import json
import uuid

from functools import wraps
from flask import request, jsonify, Flask, current_app
from urllib.parse import urlparse
from sqlalchemy.sql import text
from web_crawl_index import config
from web_crawl_index.db import init_db_session_class
from web_crawl_index.utils.utils import get_dtable_uuid_by_api_token

application = Flask(__name__)

logger = logging.getLogger(__name__)


# Initialize database session
db_session = init_db_session_class()

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


@application.route('/add-init-crawl-site-task', methods=['GET'])
@auth_required
def add_init_crawl_site_task():
    site_id = request.args.get('site_id')
    application.web_crawl_manager.add_crawl_site_task(site_id)

    return jsonify({'success': True}), 200


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

    if not query:
        return {'error_msg': 'query invalid.'}, 400

    if not project_uuid:
        return {'error_msg': 'project_uuid invalid.'}, 400

    try:
        count = int(data.get('count'))
    except:
        count = 20

    results = application.site_index_manager.search(project_uuid, query, start=0, size=count)

    return {'results': results}, 200


@application.route('/discourse/webhook/<forum_name>', methods=['POST'])
@auth_required
def discourse_webhook(forum_name):
    """Handle Discourse webhook callback"""
    # Verify forum exists by name
    with db_session() as session:
        sql = "SELECT * FROM discourse_forum WHERE name = :name"
        forum = session.execute(text(sql), {'name': forum_name}).fetchone()
        if not forum:
            return jsonify({'status': 'error', 'message': f'Forum with name {forum_name} not found'}), 404
    
    # Get webhook data
    data = request.json
    if not data:
        return jsonify({'status': 'error', 'message': 'No data received'}), 400
    
    # Extract event type from headers (Discourse standard)
    event_type = request.headers.get('X-Discourse-Event')
    if not event_type:
        return jsonify({'status': 'error', 'message': 'Event type not specified'}), 400
    
    topic_id = data.get('topic', {}).get('id')
    if not topic_id:
        logger.error(f"Could not extract topic_id for event {event_type}, data: {data}")
        return jsonify({'status': 'error', 'message': 'Topic ID not found in data for relevant event'}), 400

    try:
        discourse_manager = current_app.discourse_manager
        
        success = discourse_manager.process_webhook_event(
            forum_id=forum.id,
            topic_id=topic_id,
            event_type=event_type,
            data=data
        )
        
        if success:
            return jsonify({'status': 'success', 'message': 'Webhook event processed successfully'}), 200
        else:
            return jsonify({'status': 'error', 'message': 'Failed to process webhook event'}), 500
            
    except Exception as e:
        logger.exception(f"Error processing webhook event: {str(e)}")
        return jsonify({'status': 'error', 'message': 'Internal server error'}), 500


@application.route('/discourse/forum', methods=['POST'])
# @auth_required
def create_discourse_forum():
    """Create new Discourse forum"""
    data = request.json
    if not data:
        return jsonify({
            'status': 'error', 
            'message': 'Request body is required'
        }), 400
    
    # Validate required fields
    required_fields = ['name', 'url', 'api_key', 'api_username', 'dtable_api_token']
    missing_fields = [field for field in required_fields if field not in data]
    if missing_fields:
        return jsonify({
            'status': 'error', 
            'message': f'Missing required fields: {", ".join(missing_fields)}'
        }), 400

    try:
        parsed_url = urlparse(data['url'])
        if not parsed_url.scheme or not parsed_url.netloc:
            raise ValueError("Invalid URL")
    except:
        return jsonify({'status': 'error', 'message': 'Invalid URL format'}), 400

    # Check if forum name already exists
    with db_session() as session:
        # Check if URL already exists (using base_url field)
        sql = "SELECT * FROM discourse_forum WHERE base_url = :base_url"
        existing_url = session.execute(text(sql), {'base_url': data['url']}).fetchone()
        if existing_url:
            # If status is 'failed', allow overwrite by deleting the existing record and its dtable
            if existing_url.status == 'failed':
                logger.info(f"Found existing forum with failed status for URL {data['url']}, deleting for re-crawl")
                
                # Delete the dtable table if it exists
                if existing_url.dtable_uuid and existing_url.table_name:
                    try:
                        from web_crawl_index.utils.dtable_server_api import DTableServerAPI
                        from web_crawl_index.utils.utils import get_access_token_by_api_token
                        
                        access_token = get_access_token_by_api_token(existing_url.dtable_api_token)
                        if access_token:
                            dtable_server_api = DTableServerAPI(
                                dtable_server_url=config.INNER_DTABLE_SERVER_URL,
                                dtable_uuid=existing_url.dtable_uuid,
                                access_token=access_token
                            )
                            dtable_server_api.delete_table(existing_url.table_name)
                            logger.info(f"Deleted dtable table {existing_url.table_name} for failed forum {existing_url.id}")
                    except Exception as e:
                        logger.warning(f"Failed to delete dtable table for forum {existing_url.id}: {e}")
                
                # Delete the forum record from database
                delete_sql = "DELETE FROM discourse_forum WHERE id = :forum_id"
                session.execute(text(delete_sql), {'forum_id': existing_url.id})
                session.commit()
            else:
                return jsonify({
                    'status': 'error', 
                    'message': f'Forum URL "{data["url"]}" already exists with status: {existing_url.status}'
                }), 400

        sql = "SELECT * FROM discourse_forum WHERE name = :name"
        existing_name = session.execute(text(sql), {'name': data['name']}).fetchone()
        if existing_name:
            # If it's the same record we just deleted, skip this check
            if not (existing_url and existing_url.status == 'failed' and existing_name.id == existing_url.id):
                return jsonify({
                    'status': 'error', 
                    'message': f'Forum name "{data["name"]}" already exists'
                }), 400

    # Check dtable access token
    dtable_uuid = get_dtable_uuid_by_api_token(data['dtable_api_token'])
    if not dtable_uuid:
        return jsonify({
            'status': 'error',
            'message': 'Invalid dtable API token'
        }), 400

    try:
        # Create new forum using direct SQL
        with db_session() as session:
            table_name = str(uuid.uuid4())
            
            sql = """
                INSERT INTO discourse_forum 
                (name, base_url, api_key, api_username, dtable_api_token, description, dtable_uuid, table_name, created_at, updated_at, status)
                VALUES (:name, :base_url, :api_key, :api_username, :dtable_api_token, :description, :dtable_uuid, :table_name, NOW(), NOW(), 'pending')
            """
            result = session.execute(text(sql), {
                'name': data['name'],
                'base_url': data['url'],
                'api_key': data['api_key'],
                'api_username': data['api_username'],
                'dtable_api_token': data['dtable_api_token'],
                'description': data.get('description', ''),
                'dtable_uuid': dtable_uuid,
                'table_name': table_name
            })
            session.commit()
            
            # Get the newly created forum ID
            new_forum_id = result.lastrowid
            
            # Fetch the created forum data
            sql = "SELECT * FROM discourse_forum WHERE id = :id"
            new_forum = session.execute(text(sql), {'id': new_forum_id}).fetchone()

        # If discourse_manager exists, trigger initial sync
        if hasattr(current_app, 'discourse_manager'):
            try:
                current_app.discourse_manager.sync_forum(new_forum_id)
            except Exception as e:
                logger.warning(f"Failed to start initial sync for forum {new_forum_id}: {e}")

        # Convert forum data to dict format
        forum_dict = {
            'id': new_forum.id,
            'name': new_forum.name,
            'base_url': new_forum.base_url,
            'api_username': new_forum.api_username,
            'description': new_forum.description,
            'dtable_uuid': new_forum.dtable_uuid,
            'table_name': new_forum.table_name,
            'created_at': new_forum.created_at.isoformat() if new_forum.created_at else None,
            'updated_at': new_forum.updated_at.isoformat() if new_forum.updated_at else None,
            'last_synced_at': new_forum.last_synced_at.isoformat() if new_forum.last_synced_at else None,
            'status': new_forum.status
        }

        return jsonify({
            'status': 'success', 
            'message': 'Discourse forum created successfully', 
            'data': forum_dict
        }), 201
        
    except Exception as e:
        logger.exception(f"Error creating forum: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f'Failed to create forum: {str(e)}'
        }), 500
