import json

from dataclasses import dataclass
from typing import Any
from web_crawl_index.utils.constants import ColumnTypes


@dataclass
class DTableColumn:
    name: str
    column_type: str
    default_value: Any = None


@dataclass
class DiscourseTopicSchema:
    
    COLUMNS = [
        DTableColumn('topic_id', ColumnTypes.NUMBER),
        DTableColumn('title', ColumnTypes.TEXT),
        DTableColumn('slug', ColumnTypes.TEXT),
        DTableColumn('views', ColumnTypes.NUMBER, default_value=0),
        DTableColumn('category_id', ColumnTypes.NUMBER),
        DTableColumn('posts', ColumnTypes.LONG_TEXT, default_value=[]),
    ]
    
    @classmethod
    def get_dtable_columns(cls):
        return [
            {
                'column_name': col.name,
                'column_type': col.column_type
            }
            for col in cls.COLUMNS
        ]
    
    @classmethod
    def map_topic_data_to_row(cls, topic_data):
        row = {}
        
        field_mapping = {
            'topic_id': lambda d: d.get('id', ''),
            'title': lambda d: d.get('title', ''),
            'slug': lambda d: d.get('slug', ''),
            'views': lambda d: d.get('views', 0),
            'category_id': lambda d: d.get('category_id'),
            'posts': lambda d: json.dumps(d.get('posts', []).get('posts', [])),
        }
        
        for col in cls.COLUMNS:
            if col.name in field_mapping:
                try:
                    value = field_mapping[col.name](topic_data)
                    row[col.name] = value if value is not None else col.default_value
                except Exception:
                    row[col.name] = col.default_value
            else:
                row[col.name] = col.default_value
        
        return row

@dataclass
class DTableManager:
    
    @staticmethod
    def create_discourse_table(dtable_api, table_name: str) -> bool:
        try:
            columns = DiscourseTopicSchema.get_dtable_columns()
            dtable_api.add_table(table_name, lang='en', columns=columns)
            return True
        except Exception as e:
            raise Exception(f"Failed to create table '{table_name}': {e}")
    
    @staticmethod
    def batch_insert_topics(dtable_db_api, table_name, topics_data):
        try:
            rows = [
                DiscourseTopicSchema.map_topic_data_to_row(topic)
                for topic in topics_data
            ]
            dtable_db_api.add_rows_into_big_data_backend(table_name=table_name, rows=rows)
            return True
        except Exception as e:
            raise Exception(f"Failed to insert topics into table '{table_name}': {e}")

    @staticmethod
    def delete_big_data_table_row(dtable_db_api, table_name, topic_id):
        dtable_db_api.query(
            f"DELETE FROM `{table_name}` WHERE `topic_id` = {topic_id}"
        )
        return True
    
    @staticmethod
    def update_topic_in_table(dtable_db_api, table_name: str, topic_data):
        try:
            topic_id = topic_data.get('id')
            if not topic_id:
                raise ValueError("Topic ID is required for update")
            
            DTableManager.delete_big_data_table_row(dtable_db_api, table_name, topic_id)
            
            DTableManager.batch_insert_topics(dtable_db_api, table_name, [topic_data])
            
            return True
        except Exception as e:
            raise Exception(f"Failed to update topic {topic_data.get('id', 'unknown')} in table '{table_name}': {e}")
