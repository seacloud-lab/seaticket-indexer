import requests
import json


def parse_response(response):
    if response.status_code >= 400:
        raise ConnectionError(response.status_code, response.text)
    else:
        try:
            data = json.loads(response.text)
            return data
        except:
            pass


class DTableServerAPI():
    def __init__(self, dtable_server_url, dtable_uuid, access_token):
        self.dtable_server_url = dtable_server_url
        self.dtable_uuid = dtable_uuid
        self.access_token = access_token
        self.headers = {
            'Authorization': f'Token {self.access_token}',
        }

        self.timeout = 30

    def add_table(self, table_name, lang='en', columns=None):
        """Creates a new table in a base. Optionally, you can already add some columns to the base.
        Args:
            table_name (str): The name of the table to be created.
            lang(str): The language of the table, currently supports 'en' for English and 'zh-cn' for Chinese.
            columns (list): A list of dictionaries representing the columns to be added to the table.
        """
        url = self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/tables/?from=sea_qa'
        json_data = {
            'table_name': table_name,
            'lang': lang,
        }
        if columns:
            json_data['columns'] = columns
        response = requests.post(url, json=json_data, headers=self.headers, timeout=self.timeout)
        return parse_response(response)

    def delete_table(self, table_name):
        """Deletes a table from a base.
        Args:
            table_name (str): The name of the table to be deleted.
        """
        url = self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/tables' + '/?from=sea_qa'
        json_data = {
            'table_name': table_name,
        }
        response = requests.delete(url, json=json_data, headers=self.headers, timeout=self.timeout)
        return parse_response(response)
