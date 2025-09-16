from seaqa_indexer.utils.constants import PropertyTypes


class DiscourseTopicsTable(object):
    def __init__(self, table_id, name):
        self.id = table_id
        self.name = name

    @property
    def columns(self):
        return DiscourseTopicsColumns()


class DiscourseTopicsColumns(object):
    def __init__(self):
        self.topic_id = DiscourseColumn('topic_id', PropertyTypes.INT)
        self.title = DiscourseColumn('title', PropertyTypes.TEXT)
        self.slug = DiscourseColumn('slug', PropertyTypes.TEXT)
        self.views = DiscourseColumn('views', PropertyTypes.INT)
        self.category_id = DiscourseColumn('category_id', PropertyTypes.INT)
        self.bumped_at = DiscourseColumn('bumped_at', PropertyTypes.DATETIME)
        self.deleted = DiscourseColumn('deleted', PropertyTypes.BOOL)
        self.updated_at = DiscourseColumn('updated_at', PropertyTypes.DATETIME)


class DiscourseRepliesTable(object):
    def __init__(self, table_id, name):
        self.id = table_id
        self.name = name

    @property
    def columns(self):
        return DiscourseRepliesColumns()


class DiscourseRepliesColumns(object):
    def __init__(self):
        self.topic_id = DiscourseColumn('topic_id', PropertyTypes.INT)
        self.post_number = DiscourseColumn('post_number', PropertyTypes.INT)
        self.content = DiscourseColumn('content', PropertyTypes.TEXT)
        self.author = DiscourseColumn('author', PropertyTypes.TEXT)
        self.updated_at = DiscourseColumn('updated_at', PropertyTypes.DATETIME)


class DiscourseColumn(object):
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


# discourse table instances
DISCOURSE_TOPICS_TABLE = DiscourseTopicsTable('0000', 'DiscourseTopics')
DISCOURSE_REPLIES_TABLE = DiscourseRepliesTable('0000', 'DiscourseReplies')

DISCOURSE_TOPICS_COLUMNS = [
    DISCOURSE_TOPICS_TABLE.columns.topic_id.to_dict(),
    DISCOURSE_TOPICS_TABLE.columns.title.to_dict(),
    DISCOURSE_TOPICS_TABLE.columns.slug.to_dict(),
    DISCOURSE_TOPICS_TABLE.columns.views.to_dict(),
    DISCOURSE_TOPICS_TABLE.columns.category_id.to_dict(),
    DISCOURSE_TOPICS_TABLE.columns.bumped_at.to_dict(),
    DISCOURSE_TOPICS_TABLE.columns.deleted.to_dict(),
    DISCOURSE_TOPICS_TABLE.columns.updated_at.to_dict(),
]

DISCOURSE_REPLIES_COLUMNS = [
    DISCOURSE_REPLIES_TABLE.columns.topic_id.to_dict(),
    DISCOURSE_REPLIES_TABLE.columns.post_number.to_dict(),
    DISCOURSE_REPLIES_TABLE.columns.content.to_dict(),
    DISCOURSE_REPLIES_TABLE.columns.author.to_dict(),
    DISCOURSE_REPLIES_TABLE.columns.updated_at.to_dict(),
]
