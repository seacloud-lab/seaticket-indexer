import requests

class DTableWebAPI(object):

    def __init__(self, dtable_web_url, time_out=30):
        self.dtable_web_url = dtable_web_url.rstrip('/')
        self.timeout = time_out

    def gen_headers(self, access_token):
        return {"Authorization": f"Bearer {access_token}"}

    def open_big_data_storage(self, dtable_uuid, access_token):
        """Open the big data storage for a specific dtable."""
        self.headers = self.gen_headers(access_token)
        json_data = {
            "dtable_uuid": dtable_uuid,
        }
        url = f"{self.dtable_web_url}/api/v2.1/sea-qa/internal/big-data-storage/"
        response = requests.post(url, json=json_data, headers=self.headers, timeout=self.timeout)
        if response.status_code == 200:
            return response.json()
        else:
            raise ConnectionError(response.status_code, response.text)
