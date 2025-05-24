from flask import Blueprint, request, jsonify, current_app
from models import db, Project, Website
from urllib.parse import urlparse, unquote
import logging

logger = logging.getLogger(__name__)

# Create Blueprint
api_bp = Blueprint('api', __name__, url_prefix='/api')


# Project related APIs
@api_bp.route('/projects', methods=['GET'])
def get_projects():
    """Get all projects list"""
    projects = Project.query.all()
    return jsonify(
        {'status': 'success', 'data': [project.to_dict() for project in projects]}
    )


@api_bp.route('/projects/<string:project_name>', methods=['GET'])
def get_project(project_name):
    """Get specific project details"""
    project = Project.query.filter_by(name=project_name).first()
    if not project:
        return (
            jsonify({'status': 'error', 'message': f'Project "{project_name}" does not exist'}),
            404,
        )

    return jsonify({'status': 'success', 'data': project.to_dict()})


@api_bp.route('/projects', methods=['POST'])
def create_project():
    """Create a new project"""
    data = request.json
    if not data or 'name' not in data:
        return jsonify({'status': 'error', 'message': 'Project name is required'}), 400

    # Check if project name already exists
    existing = Project.query.filter_by(name=data['name']).first()
    if existing:
        return (
            jsonify(
                {'status': 'error', 'message': f'Project name "{data["name"]}" already exists'}
            ),
            400,
        )

    # Create new project
    project = Project(name=data['name'], description=data.get('description', ''))
    db.session.add(project)
    db.session.commit()

    return (
        jsonify(
            {'status': 'success', 'message': 'Project created successfully', 'data': project.to_dict()}
        ),
        201,
    )


@api_bp.route('/projects/<string:project_name>', methods=['DELETE'])
def delete_project(project_name):
    """Delete a specific project"""
    project = Project.query.filter_by(name=project_name).first()
    if not project:
        return (
            jsonify({'status': 'error', 'message': f'Project "{project_name}" does not exist'}),
            404,
        )

    # Delete all websites under the project
    Website.query.filter_by(project_id=project.id).delete()

    # Delete the project
    db.session.delete(project)
    db.session.commit()

    return jsonify(
        {
            'status': 'success',
            'message': f'Project "{project_name}" and all its websites have been successfully deleted',
        }
    )


# Website related APIs
@api_bp.route('/projects/<string:project_name>/websites', methods=['GET'])
def get_websites(project_name):
    """Get all websites under a project"""
    project = Project.query.filter_by(name=project_name).first()
    if not project:
        return (
            jsonify({'status': 'error', 'message': f'Project "{project_name}" does not exist'}),
            404,
        )

    websites = Website.query.filter_by(project_id=project.id).all()
    return jsonify(
        {'status': 'success', 'data': [website.to_dict() for website in websites]}
    )


@api_bp.route('/projects/<string:project_name>/websites', methods=['POST'])
def add_website(project_name):
    """Add a new website to a project"""
    project = Project.query.filter_by(name=project_name).first()
    if not project:
        return (
            jsonify({'status': 'error', 'message': f'Project "{project_name}" does not exist'}),
            404,
        )

    data = request.json
    if not data or 'url' not in data:
        return jsonify({'status': 'error', 'message': 'Website URL is required'}), 400

    # Validate URL format
    try:
        parsed_url = urlparse(data['url'])
        if not parsed_url.scheme or not parsed_url.netloc:
            raise ValueError("Invalid URL")
    except:
        return jsonify({'status': 'error', 'message': 'Invalid URL format'}), 400

    # Check if URL already exists in the project
    existing = Website.query.filter_by(project_id=project.id, url=data['url']).first()
    if existing:
        return (
            jsonify(
                {'status': 'error', 'message': f'URL "{data["url"]}" already exists in this project'}
            ),
            400,
        )

    # Create new website
    website = Website(
        url=data['url'], sitemap_url=data.get('sitemap_url'), project_id=project.id
    )
    db.session.add(website)
    db.session.commit()

    return (
        jsonify(
            {'status': 'success', 'message': 'Website added successfully', 'data': website.to_dict()}
        ),
        201,
    )


@api_bp.route(
    '/projects/<string:project_name>/websites/url/<path:website_url>', methods=['DELETE']
)
def delete_website_by_url(project_name, website_url):
    """Delete a specific website from a project by URL"""
    project = Project.query.filter_by(name=project_name).first()
    if not project:
        return (
            jsonify({'status': 'error', 'message': f'Project "{project_name}" does not exist'}),
            404,
        )
    
    # Decode URL
    decoded_url = unquote(website_url)
    
    # Find website
    website = Website.query.filter_by(project_id=project.id, url=decoded_url).first()
    if not website:
        return (
            jsonify(
                {
                    'status': 'error',
                    'message': f'Website URL "{decoded_url}" does not exist in project "{project_name}"',
                }
            ),
            404,
        )

    db.session.delete(website)
    db.session.commit()

    return jsonify(
        {
            'status': 'success',
            'message': f'Website "{website.url}" has been removed from project "{project_name}"',
        }
    )


@api_bp.route(
    '/projects/<string:project_name>/websites/url/<path:website_url>/crawl', methods=['POST']
)
def crawl_website_by_url(project_name, website_url):
    """Crawl a specific website in a project by URL"""
    project = Project.query.filter_by(name=project_name).first()
    if not project:
        return (
            jsonify({'status': 'error', 'message': f'Project "{project_name}" does not exist'}),
            404,
        )
    
    # Decode URL
    decoded_url = unquote(website_url)
    
    # Find website
    website = Website.query.filter_by(project_id=project.id, url=decoded_url).first()
    if not website:
        return (
            jsonify(
                {
                    'status': 'error',
                    'message': f'Website URL "{decoded_url}" does not exist in project "{project_name}"',
                }
            ),
            404,
        )

    # Get crawler manager
    crawler_manager = current_app.crawler_manager

    # Start crawler
    success = crawler_manager.start_crawler(website.id)
    if success:
        return jsonify(
            {'status': 'success', 'message': f'Crawling task for website "{website.url}" has been started'}
        )
    else:
        return (
            jsonify(
                {
                    'status': 'error',
                    'message': f'Failed to start crawling task for website "{website.url}", a crawler might already be running',
                }
            ),
            400,
        )


@api_bp.route('/web-crawl', methods=['POST'])
def add_and_crawl_website():
    """Add and crawl a website based on project name and URL"""
    data = request.json
    if not data or 'url' not in data or 'project_name' not in data:
        return jsonify({'status': 'error', 'message': 'URL and project name are required'}), 400

    # Get crawler manager
    crawler_manager = current_app.crawler_manager

    # Start crawler
    success, message = crawler_manager.start_crawler_by_url_and_project(
        data['url'], data['project_name'], data.get('sitemap_url')
    )

    if success:
        return jsonify({'status': 'success', 'message': message})
    else:
        return jsonify({'status': 'error', 'message': message}), 400
