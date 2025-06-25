import logging
import jwt
from functools import wraps
from flask import request, jsonify, Flask

from web_crawl_index import config

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


@application.route('/add-init-crawl-site-task', methods=['GET'])
@auth_required
def add_init_crawl_site_task():
    site_id = request.args.get('site_id')
    application.web_crawl_index_manager.add_crawl_site_task(site_id)

    return jsonify({'success': True}), 200
