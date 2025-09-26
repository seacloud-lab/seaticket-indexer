
class PropertyTypes:
    TEXT = 'text'
    DATETIME = 'datetime'
    INT = 'int64'
    FLOAT = 'float64'
    SINGLE_SELECT = 'single-select'
    MULTIPLE_SELECT = 'multiple-select'
    BOOL = 'bool'


class SelectTypes:
    state = {
      "options": [
        {
          "id": "0001",
          "name": "open"
        },
        {
          "id": "0002",
          "name": "closed"
        }
      ]
    }

    state_reason = {
      "options": [
        {
          "id": "0001",
          "name": "completed"
        },
        {
          "id": "0002",
          "name": "not_planned"
        },
        {
          "id": "0003",
          "name": "duplicate"
        },
        {
          "id": "0004",
          "name": "reopened"
        }
      ]
    }

    issue_type = {
      "options": [
        {
          "id": "0001",
          "name": "Bug",
        },
        {
          "id": "0002",
          "name": "Feature"
        },
        {
          "id": "0003",
          "name": "Task"
        }
      ]
    }



class MappedColumn(object):
    def __init__(self, name, type, data=None):
        self.name = name
        self.type = type
        self.data = data

    def to_dict(self, data=None):
        column_data = {
            'name': self.name,
            'type': self.type,
        }
        if self.data:
            column_data['data'] = self.data

        if data:
            column_data['data'] = data

        return column_data


class BaseModel:
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls._meta = {
            'fields': []
        }

        for name, attr in cls.__dict__.items():
            if isinstance(attr, MappedColumn):
                cls._meta['fields'].append(attr)

    @classmethod
    def get_fields(cls):
        return cls._meta['fields'].copy()

    def __init__(self, **kwargs):
        for field in self.__class__._meta['fields']:
            setattr(self, field.name, field)

    def __repr__(self):
        fields = ", ".join([f"{k}={v!r}" for k, v in self.__dict__.items()])
        return f"{self.__class__.__name__}({fields})"


class DiscourseTopicsTable(BaseModel):
    topic_id = MappedColumn('topic_id', PropertyTypes.INT)
    title = MappedColumn('title', PropertyTypes.TEXT)
    slug = MappedColumn('slug', PropertyTypes.TEXT)
    views = MappedColumn('views', PropertyTypes.INT)
    category_id = MappedColumn('category_id', PropertyTypes.INT)
    bumped_at = MappedColumn('bumped_at', PropertyTypes.DATETIME)
    deleted = MappedColumn('deleted', PropertyTypes.BOOL)
    updated_at = MappedColumn('updated_at', PropertyTypes.DATETIME)
    created_at = MappedColumn('created_at', PropertyTypes.DATETIME)


class DiscourseRepliesTable(BaseModel):
    topic_id = MappedColumn('topic_id', PropertyTypes.INT)
    post_number = MappedColumn('post_number', PropertyTypes.INT)
    content = MappedColumn('content', PropertyTypes.TEXT)
    author = MappedColumn('author', PropertyTypes.TEXT)
    updated_at = MappedColumn('updated_at', PropertyTypes.DATETIME)


class WebCrawlTable(BaseModel):
    url = MappedColumn('url', PropertyTypes.TEXT)
    title = MappedColumn('title', PropertyTypes.TEXT)
    etag = MappedColumn('etag', PropertyTypes.TEXT)
    last_modified = MappedColumn('last_modified', PropertyTypes.DATETIME)
    updated_at = MappedColumn('updated_at', PropertyTypes.DATETIME)
    deleted = MappedColumn('deleted', PropertyTypes.BOOL)
    hash = MappedColumn('hash', PropertyTypes.TEXT)


class GithubIssuesTable(BaseModel):
    issue_id = MappedColumn('issue_id', PropertyTypes.INT)
    issue_number = MappedColumn('issue_number', PropertyTypes.INT)
    title = MappedColumn('title', PropertyTypes.TEXT)
    body = MappedColumn('body', PropertyTypes.TEXT)
    state = MappedColumn('state', PropertyTypes.SINGLE_SELECT, SelectTypes.state)
    state_reason = MappedColumn('state_reason', PropertyTypes.SINGLE_SELECT, SelectTypes.state_reason)
    labels = MappedColumn('labels', PropertyTypes.MULTIPLE_SELECT)
    issue_type = MappedColumn('issue_type', PropertyTypes.SINGLE_SELECT, SelectTypes.issue_type)
    author = MappedColumn('author', PropertyTypes.TEXT)
    assignees = MappedColumn('assignees', PropertyTypes.TEXT)
    url = MappedColumn('url', PropertyTypes.TEXT)
    created_at = MappedColumn('created_at', PropertyTypes.DATETIME)
    updated_at = MappedColumn('updated_at', PropertyTypes.DATETIME)
    closed_at = MappedColumn('closed_at', PropertyTypes.DATETIME)
    comments_count = MappedColumn('comments_count', PropertyTypes.INT)
    deleted = MappedColumn('deleted', PropertyTypes.BOOL)


class GithubIssueCommentsTable(BaseModel):
    comment_id = MappedColumn('comment_id', PropertyTypes.INT)
    issue_id = MappedColumn('issue_id', PropertyTypes.INT)
    author = MappedColumn('author', PropertyTypes.TEXT)
    body = MappedColumn('body', PropertyTypes.TEXT)
    created_at = MappedColumn('created_at', PropertyTypes.DATETIME)
    updated_at = MappedColumn('updated_at', PropertyTypes.DATETIME)
    deleted = MappedColumn('deleted', PropertyTypes.BOOL)
