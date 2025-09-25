
class PropertyTypes:
    TEXT = 'text'
    DATETIME = 'datetime'
    INT = 'int64'
    FLOAT = 'float64'
    SINGLE_SELECT = 'single-select'
    MULTIPLE_SELECT = 'multiple-select'
    BOOL = 'bool'


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
