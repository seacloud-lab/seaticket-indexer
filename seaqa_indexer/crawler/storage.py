import json
import boto3
import logging
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError
from seaqa_indexer.utils import uuid_str_to_36_chars

from seaqa_indexer.config import (
    S3_HOST,
    S3_FILE_BUCKET,
    S3_WEB_CRAWL_BUCKET,
    S3_KEY_ID,
    S3_SECRET_KEY,
)

logger = logging.getLogger(__name__)

KEY_PREFIX_FORMAT = "{project_uuid}/{site_id}/"

class BaseStorage:
    def write_json(self, site_id, project_uuid, filename, data):
        raise NotImplementedError

    def read_json(self, site_id, project_uuid, filename):
        raise NotImplementedError

    def delete_file(self, site_id, project_uuid, filename):
        raise NotImplementedError

    def list_files(self, site_id, project_uuid):
        raise NotImplementedError

    def delete_site(self, site_id, project_uuid):
        """Delete all crawled files for a site (project_uuid/site_id)."""
        raise NotImplementedError

    def delete_project_crawl_data(self, project_uuid):
        """Delete all crawled files for a project (project_uuid/*)."""
        raise NotImplementedError

    def delete_project_attachment(self, project_uuid):
        """Delete all uploaded files for a project (/projects/{project_uuid}/*)."""
        raise NotImplementedError


class S3Storage(BaseStorage):
    def __init__(self):
        session = boto3.session.Session()
        self.client = session.client(
            's3',
            aws_access_key_id=S3_KEY_ID,
            aws_secret_access_key=S3_SECRET_KEY,
            endpoint_url=S3_HOST,
            config=BotoConfig(signature_version='s3v4', s3={'addressing_style': 'path'}),
        )
        self.bucket = S3_WEB_CRAWL_BUCKET
        self.file_bucket = S3_FILE_BUCKET

    def ping(self):
        """Check if S3 bucket is accessible. Returns True if ok, else False."""
        try:
            self.client.head_bucket(Bucket=self.bucket)
            return True
        except Exception as e:
            logger.warning("S3 ping failed for bucket %s: %s", self.bucket, e)
            return False

    @staticmethod
    def _key(project_uuid, site_id, filename):
        return KEY_PREFIX_FORMAT.format(project_uuid=project_uuid, site_id=site_id) + filename

    def write_json(self, site_id, project_uuid, filename, data):
        key = self._key(project_uuid, site_id, filename)
        body = json.dumps(data, ensure_ascii=False).encode('utf-8')
        self.client.put_object(Bucket=self.bucket, Key=key, Body=body, ContentType='application/json; charset=utf-8')

    def upload_file(self, site_id, project_uuid, filename, data):
        key = self._key(project_uuid, site_id, filename)
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data)

    def read_json(self, site_id, project_uuid, filename):
        key = self._key(project_uuid, site_id, filename)
        try:
            resp = self.client.get_object(Bucket=self.bucket, Key=key)
            content = resp['Body'].read()
            return json.loads(content)
        except ClientError as e:
            if e.response['Error']['Code'] in ('NoSuchKey', '404'):
                return None
            raise e

        except Exception as e:
            logger.error('read_json error: %s', e)
            return None

    def delete_file(self, site_id, project_uuid, filename):
        key = self._key(project_uuid, site_id, filename)
        self.client.delete_object(Bucket=self.bucket, Key=key)

    def batch_delete_files(self, site_id, project_uuid, files):
        prefix = KEY_PREFIX_FORMAT.format(project_uuid=project_uuid, site_id=site_id)
        objects = [{'Key': prefix + file} for file in files]
        self.client.delete_objects(Bucket=self.bucket, Delete={'Objects': objects})

    def list_files(self, site_id, project_uuid):
        return list(self.iter_files(site_id, project_uuid))

    def iter_files(self, site_id, project_uuid):
        prefix = KEY_PREFIX_FORMAT.format(project_uuid=project_uuid, site_id=site_id)
        continuation_token = None
        while True:
            params = {'Bucket': self.bucket, 'Prefix': prefix}
            if continuation_token:
                params['ContinuationToken'] = continuation_token
            resp = self.client.list_objects_v2(**params)
            for obj in resp.get('Contents', []) or []:
                key = obj['Key']
                if not key.endswith('.json'):
                    continue
                yield key[len(prefix):]
            if resp.get('IsTruncated'):
                continuation_token = resp.get('NextContinuationToken')
            else:
                break

    def _delete_by_prefix(self, bucket, prefix):
        continuation_token = None
        to_delete = []
        while True:
            params = {'Bucket': bucket, 'Prefix': prefix}
            if continuation_token:
                params['ContinuationToken'] = continuation_token
            resp = self.client.list_objects_v2(**params)
            contents = resp.get('Contents', []) or []
            if contents:
                # batch delete up to 1000
                for obj in contents:
                    to_delete.append({'Key': obj['Key']})
                    if len(to_delete) == 1000:
                        self.client.delete_objects(Bucket=bucket, Delete={'Objects': to_delete})
                        to_delete = []
            if resp.get('IsTruncated'):
                continuation_token = resp.get('NextContinuationToken')
            else:
                break
        if to_delete:
            self.client.delete_objects(Bucket=bucket, Delete={'Objects': to_delete})

    def delete_site(self, site_id, project_uuid):
        prefix = KEY_PREFIX_FORMAT.format(project_uuid=project_uuid, site_id=site_id)
        self._delete_by_prefix(self.bucket, prefix)

    def delete_project_crawl_data(self, project_uuid):
        prefix = f"{project_uuid}/"
        self._delete_by_prefix(self.bucket, prefix)

    def delete_project_attachment(self, project_uuid):
        if not self.file_bucket:
            return
        if len(project_uuid) == 32:
            project_uuid = uuid_str_to_36_chars(project_uuid)
        prefix = f"projects/{project_uuid}/"
        self._delete_by_prefix(self.file_bucket, prefix)


_storage = None


def get_storage():
    global _storage
    if _storage is not None:
        return _storage
    # Determine if S3 is configured
    s3_configured = all([S3_HOST, S3_WEB_CRAWL_BUCKET, S3_KEY_ID, S3_SECRET_KEY])
    if s3_configured:
        try:
            s3 = S3Storage()
            if s3.ping():
                _storage = s3
                return _storage
            else:
                logger.warning("S3 configured but not reachable.")
        except Exception as e:
            logger.warning("Failed to initialize S3Storage: %s", e)
    else:
        logger.warning("S3 not configured.")

    return None
