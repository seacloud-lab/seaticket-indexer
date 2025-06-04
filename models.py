from flask_sqlalchemy import SQLAlchemy
from datetime import datetime


db = SQLAlchemy()


class Project(db.Model):
    """Project model, used for organizing a collection of websites"""
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False, unique=True)
    description = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.now)
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now)
    
    # Associated websites
    websites = db.relationship('Website', backref='project', lazy=True, cascade='all, delete-orphan')
    
    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'description': self.description,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
            'website_count': len(self.websites)
        }


class Website(db.Model):
    """Website model, belongs to a project"""
    id = db.Column(db.Integer, primary_key=True)
    url = db.Column(db.String(255), nullable=False)
    sitemap_url = db.Column(db.String(255), nullable=True)
    project_id = db.Column(db.Integer, db.ForeignKey('project.id'), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    last_crawled_at = db.Column(db.DateTime, nullable=True)
    status = db.Column(db.String(20), default='pending')  # pending, crawling, completed, failed
    
    def to_dict(self):
        return {
            'id': self.id,
            'url': self.url,
            'sitemap_url': self.sitemap_url,
            'project_id': self.project_id,
            'created_at': self.created_at.isoformat(),
            'last_crawled_at': self.last_crawled_at.isoformat() if self.last_crawled_at else None,
            'status': self.status
        }
