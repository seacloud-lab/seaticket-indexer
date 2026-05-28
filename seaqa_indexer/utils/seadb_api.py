import time
import jwt
import requests

from seaqa_indexer.config import SEADB_SERVER_ACCESS_TOKEN, SEADB_SERVER_URL
from seaqa_indexer.utils import uuid_str_to_36_chars

SEADB_COMPONENT_USERNAME = 'seaqa-indexer'


def parse_response(response):
    if response.status_code >= 400 or response.status_code < 200:
        raise ConnectionError(response.status_code, response.text)
    else:
        try:
            return response.json()
        except:
            return {}


class OptionValueInvalidException(Exception):
    def __init__(self, message="option value invalid"):
        self.message = message
        super().__init__(self.message)


class TableNotFound(Exception):
    def __init__(self, message="no such table"):
        self.message = message
        super().__init__(self.message)


class SeaDBAPI:
    def __init__(self, timeout=30, username=SEADB_COMPONENT_USERNAME):
        self.timeout = timeout
        self.secret_key = SEADB_SERVER_ACCESS_TOKEN
        self.server_url = SEADB_SERVER_URL
        self.username = username

    def gen_headers(self, base_id):
        payload = {
            'exp': int(time.time()) + 3600,
            'base_id': base_id,
            'username': self.username,
        }
        token = jwt.encode(payload, self.secret_key, algorithm='HS256')
        return {"Authorization": "Bearer %s" % token}


    def ping(self):
        url = f'{self.server_url}/ping'
        response = requests.get(url, timeout=self.timeout)
        return parse_response(response)

    # base
    def create_base(self, base_id):
        base_id = uuid_str_to_36_chars(base_id)
        headers = self.gen_headers(base_id)
        url = f'{self.server_url}/api/v1/{base_id}/base'
        response = requests.post(url, headers=headers, timeout=self.timeout)
        return parse_response(response)

    def get_base_info(self, base_id):
        base_id = uuid_str_to_36_chars(base_id)
        headers = self.gen_headers(base_id)
        url = f'{self.server_url}/api/v1/{base_id}/base-info'
        response = requests.get(url, headers=headers, timeout=self.timeout)
        return parse_response(response)

    def get_base_metadata(self, base_id):
        base_id = uuid_str_to_36_chars(base_id)
        headers = self.gen_headers(base_id)
        url = f'{self.server_url}/api/v1/{base_id}/metadata'
        response = requests.get(url, headers=headers, timeout=self.timeout)
        return parse_response(response)

    def delete_base(self, base_id):
        base_id = uuid_str_to_36_chars(base_id)
        headers = self.gen_headers(base_id)
        url = f'{self.server_url}/api/v1/{base_id}/base'
        response = requests.delete(url, headers=headers, timeout=self.timeout)
        return parse_response(response)

    # table
    def create_table(self, base_id, table_name):
        base_id = uuid_str_to_36_chars(base_id)
        headers = self.gen_headers(base_id)
        url = f'{self.server_url}/api/v1/{base_id}/tables'
        params = {
            'table_name': str(table_name)
        }
        response = requests.post(url, headers=headers, json=params, timeout=self.timeout)
        return parse_response(response)

    def delete_table(self, base_id, table_id):
        base_id = uuid_str_to_36_chars(base_id)
        headers = self.gen_headers(base_id)
        url = f'{self.server_url}/api/v1/{base_id}/tables/'
        params = {
            'table_id': table_id
        }
        response = requests.delete(url, headers=headers, json=params, timeout=self.timeout)
        return parse_response(response)

    # rows
    def insert_rows(self, base_id, table_name, rows):
        base_id = uuid_str_to_36_chars(base_id)
        headers = self.gen_headers(base_id)
        url = f'{self.server_url}/api/v1/{base_id}/rows'
        data = {
                'table_name': str(table_name),
                'rows': rows
            }
        response = requests.post(url, json=data, headers=headers, timeout=self.timeout)
        if response.status_code == 400 and 'row[0]: column' in response.text:
            raise OptionValueInvalidException(response.text)
        return parse_response(response)

    def update_rows(self, base_id, table_name, rows):
        base_id = uuid_str_to_36_chars(base_id)
        headers = self.gen_headers(base_id)
        url = f'{self.server_url}/api/v1/{base_id}/rows'
        data = {
                'table_name': table_name,
                'updates': rows
            }
        response = requests.put(url, json=data, headers=headers, timeout=self.timeout)
        if response.status_code == 400 and 'row[0]: column' in response.text:
            raise OptionValueInvalidException(response.text)
        return parse_response(response)

    def delete_rows(self, base_id, table_name, row_ids):
        base_id = uuid_str_to_36_chars(base_id)
        headers = self.gen_headers(base_id)
        url = f'{self.server_url}/api/v1/{base_id}/rows'
        data = {
                'table_name': table_name,
                'pks': row_ids
            }
        response = requests.delete(url, json=data, headers=headers, timeout=self.timeout)
        return parse_response(response)

    def query_rows(self, base_id, sql, params=[], convert_keys=True):
        base_id = uuid_str_to_36_chars(base_id)
        headers = self.gen_headers(base_id)
        post_data = {
            'sql': sql,
            'convert_keys': convert_keys
        }
        if params:
            post_data['params'] = params
        url = f'{self.server_url}/api/v1/{base_id}/query'
        response = requests.post(url, json=post_data, headers=headers, timeout=self.timeout)
        if response.status_code == 400 and 'no such table' in response.text:
            raise TableNotFound()
        return parse_response(response)

    # columns
    def add_column(self, base_id, table_id, column):
        base_id = uuid_str_to_36_chars(base_id)
        headers = self.gen_headers(base_id)
        url = f'{self.server_url}/api/v1/{base_id}/columns'
        data = {
            'table_id': table_id,
            'column_name': column['column_name'],
            'column_type': column['column_type'],
        }
        if column.get('column_data'):
            data['column_data'] = column['column_data']
        response = requests.post(url, json=data, headers=headers, timeout=self.timeout)
        return parse_response(response)

    def delete_column(self, base_id, table_id, column_key):
        base_id = uuid_str_to_36_chars(base_id)
        headers = self.gen_headers(base_id)
        url = f'{self.server_url}/api/v1/{base_id}/columns'
        data = {
            'table_id': table_id,
            'column_key': column_key,
        }
        response = requests.delete(url, json=data, headers=headers, timeout=self.timeout)
        return parse_response(response)

    def update_column(self, base_id, table_id, column_key, new_column_name=None, update_column_data=None):
        base_id = uuid_str_to_36_chars(base_id)
        headers = self.gen_headers(base_id)
        url = f'{self.server_url}/api/v1/{base_id}/columns'
        data = {
            'table_id': table_id,
            'column_key': column_key
        }
        if new_column_name:
            data['new_column_name'] = new_column_name
        if update_column_data:
            data['update_column_data'] = update_column_data
        response = requests.put(url, json=data, headers=headers, timeout=self.timeout)
        return parse_response(response)

    def create_column_index(self, base_id, table_id, columns):
        base_id = uuid_str_to_36_chars(base_id)
        headers = self.gen_headers(base_id)
        url = f'{self.server_url}/api/v1/{base_id}/index'
        data = {
            'table_id': table_id,
            'columns': columns,
        }
        response = requests.post(url, json=data, headers=headers, timeout=self.timeout)
        return parse_response(response)

    def add_column_option(self, base_id, option_data):
        base_id = uuid_str_to_36_chars(base_id)
        headers = self.gen_headers(base_id)
        url = f'{self.server_url}/api/v1/{base_id}/column-options'
        response = requests.post(url, json=option_data, headers=headers, timeout=self.timeout)
        return parse_response(response)

    def update_column_option(self, base_id, table_id, column_key, option_id=None, option_name=None, new_option_name=None, update_option_data=None):
        base_id = uuid_str_to_36_chars(base_id)
        headers = self.gen_headers(base_id)
        url = f'{self.server_url}/api/v1/{base_id}/column-options'
        data = {
            'table_id': table_id,
            'column_key': column_key,
        }

        if option_id:
            data['option_id'] = option_id
        if option_name:
            data['option_name'] = option_name
        if new_option_name:
            data['new_option_name'] = new_option_name
        if update_option_data:
            data['update_option_data'] = update_option_data

        response = requests.put(url, json=data, headers=headers, timeout=self.timeout)
        return parse_response(response)


    def delete_column_option(self, base_id, table_id, column_key, option_id=None, option_ids=None):
        base_id = uuid_str_to_36_chars(base_id)
        headers = self.gen_headers(base_id)
        url = f'{self.server_url}/api/v1/{base_id}/column-options'
        data = {
            'table_id': table_id,
            'column_key': column_key
        }
        if option_id:
            data['option_id'] = option_id
        if option_ids:
            data['option_ids'] = option_ids

        response = requests.delete(url, json=data, headers=headers, timeout=self.timeout)
        return parse_response(response)

    def update_table(self, base_id, table_id, table_name, new_table_name):
        base_id = uuid_str_to_36_chars(base_id)
        headers = self.gen_headers(base_id)
        url = f'{self.server_url}/api/v1/{base_id}/tables'
        data = {
            'table_id': table_id,
            'table_name': table_name,
            'new_table_name': new_table_name,
        }
        response = requests.put(url, json=data, headers=headers, timeout=self.timeout)
        return parse_response(response)
