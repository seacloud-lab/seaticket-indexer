import json
import logging
import requests


logger = logging.getLogger(__name__)


class RowsQueryError(Exception):
    pass


def parse_response(response):
    if response.status_code >= 400:
        raise ConnectionError(response.status_code, response.text)
    else:
        try:
            data = json.loads(response.text)
            return data
        except:
            pass


class DTableDBAPI(object):

    def __init__(self, dtable_db_url, dtable_uuid, access_token, timeout=30):
        self.dtable_db_url = dtable_db_url.rstrip('/')
        self.dtable_uuid = dtable_uuid
        self.timeout = timeout
        self.access_token = access_token
        self.headers = {
            'accept': 'application/json',
            'content-type': 'application/json',
            'authorization': f'Token {self.access_token}',
        }

    def add_rows_into_big_data_backend(self, table_name, rows):
        """Batch insert rows from the archive. 
        Note that this API will directly insert the rows into the archive storage backend of dtable-db, 
        without affecting unarchived data. 
        If you need to insert new rows into unarchived data, please use the dtable-server API.
        Args:
            table_name (str): The name of the table to insert rows into.
            rows (list): A list of dictionaries representing the rows to be inserted.
        Returns:
            dict: The response about the insert_row_count
        """
        url = f"{self.dtable_db_url}/api/v1/insert-rows/{self.dtable_uuid}/?from=sea_qa"
        json_data = {
            'table_name': table_name,
            'rows': rows
        }
        response = requests.post(url, json=json_data, headers=self.headers, timeout=self.timeout)
        return parse_response(response)

    def query(self, sql, convert=True, server_only=True):
        dtable_uuid = self.dtable_uuid
        if not sql:
            raise ValueError('sql can not be empty.')
        url = self.dtable_db_url + '/api/v1/query/' + dtable_uuid + '/?from=sea_qa'
        json_data = {'sql': sql, 'server_only': server_only, 'convert_keys': convert}
        response = requests.post(url, json=json_data, headers=self.headers, timeout=self.timeout)
        data = parse_response(response)
        if not data.get('success'):
            error_msg = 'dtable_uuid:' + dtable_uuid + '. sql:' + sql + '. '
            if response.status_code == 200:
                raise RowsQueryError(error_msg + data.get('error_message'))
            raise Exception(error_msg + data.get('error_message'))
        metadata = data.get('metadata')
        results = data.get('results')
        return results, metadata
