class CrawlStatus:
    PENDING = 'pending'
    CRAWLING = 'crawling'
    COMPLETED = 'completed'
    FAILED = 'failed'


SITE_INDEX_PREFIX = 'site_'


class ColumnTypes:
    COLLABORATOR = 'collaborator'
    NUMBER = 'number'
    DATE = 'date'
    GEOLOCATION = 'geolocation'
    CREATOR = 'creator'
    LAST_MODIFIER = 'last-modifier'
    TEXT = 'text'
    IMAGE = 'image'
    LONG_TEXT = 'long-text'
    CHECKBOX = 'checkbox'
    SINGLE_SELECT = 'single-select'
    MULTIPLE_SELECT = 'multiple-select'
    URL = 'url'
    DURATION = 'duration'
    FILE = 'file'
    EMAIL = 'email'
    RATE = 'rate'
    FORMULA = 'formula'
    LINK_FORMULA = 'link-formula'
    AUTO_NUMBER = 'auto-number'
    LINK = 'link'
    CTIME = 'ctime'
    MTIME = 'mtime'
    BUTTON = 'button'
    DEPARTMENT_SINGLE_SELECT = 'department-single-select'
    DIGITAL_SIGN = 'digital-sign'

    @classmethod
    def is_valid(cls, item):
        if not getattr(cls, 'column_types_set', None):
            cls.column_types_set = {
                getattr(cls, attr) 
                for attr in dir(cls) 
                if not attr.startswith('__') and not callable(getattr(cls, attr))
            }
        return item in getattr(cls, 'column_types_set')
