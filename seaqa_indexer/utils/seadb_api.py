import requests
import jwt
import time
import uuid

from seaqa_indexer.config import JWT_PRIVATE_KEY, SEADB_SERVER_URL


def parse_response(response):
    if response.status_code >= 400 or response.status_code < 200:
        raise ConnectionError(response.status_code, response.text)
    else:
        try:
            return response.json()
        except:
            pass

def uuid_str_to_36_chars(uuid_str):
    if isinstance(uuid_str, uuid.UUID):
        return str(uuid_str)
    if len(uuid_str) == 32:
        return str(uuid.UUID(uuid_str))
    else:
        return uuid_str

class SeaDBAPI:
    def __init__(self, username='', timeout=30):
        self.timeout = timeout
        self.secret_key = JWT_PRIVATE_KEY
        self.server_url = SEADB_SERVER_URL
        self.username = username

    def gen_headers(self, base_id):
        payload = {
            'exp': int(time.time()) + 3600,
            'base_id': base_id,
            'username': self.username
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

