import os
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

    def delete_connection_crawl_data(self, connection_id, project_uuid):
        """Delete all crawled files for a connection (project_uuid/connection_id)."""
        raise NotImplementedError

    def delete_project_crawl_data(self, project_uuid):
        """Delete all crawled files for a project (project_uuid/*)."""
        raise NotImplementedError

    def delete_project_attachment(self, project_uuid):
        """Delete all uploaded files for a project (/projects/{project_uuid}/*)."""
        raise NotImplementedError

    def get_project_storage_usage(self, project_uuid):
        """Return the current object bytes and count attributable to a project."""
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

    def upload_connection_record_file(self, project_uuid, connection_id, pk, filename, data):
        key = os.path.join(project_uuid, str(connection_id), str(pk), filename)
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

    def batch_delete_files(self, site_id, project_uuid, filenames):
        prefix = KEY_PREFIX_FORMAT.format(project_uuid=project_uuid, site_id=site_id)
        objects = [{'Key': prefix + filename} for filename in filenames]
        self._delete_objects(self.bucket, objects)

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
            if not resp.get('IsTruncated'):
                break
            continuation_token = self._get_next_continuation_token(resp)

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
                        self._delete_objects(bucket, to_delete)
                        to_delete = []
            if not resp.get('IsTruncated'):
                break
            continuation_token = self._get_next_continuation_token(resp)
        if to_delete:
            self._delete_objects(bucket, to_delete)

    def _delete_objects(self, bucket, objects):
        response = self.client.delete_objects(Bucket=bucket, Delete={'Objects': objects})
        errors = response.get('Errors') or []
        if errors:
            error_codes = sorted({error.get('Code', 'Unknown') for error in errors})
            raise RuntimeError('Failed to delete %s S3 objects, error codes: %s' % (len(errors), ', '.join(error_codes)))

    def _get_s3_prefix_storage_usage(self, bucket, prefix):
        total_size = 0
        object_count = 0
        continuation_token = None
        while True:
            params = {'Bucket': bucket, 'Prefix': prefix}
            if continuation_token:
                params['ContinuationToken'] = continuation_token
            response = self.client.list_objects_v2(**params)
            for obj in response.get('Contents') or []:
                size = obj.get('Size')
                if not isinstance(size, int) or size < 0:
                    raise ValueError('Invalid S3 object size')
                total_size += size
                object_count += 1
            if not response.get('IsTruncated'):
                break
            continuation_token = self._get_next_continuation_token(response)
        return total_size, object_count

    @staticmethod
    def _get_next_continuation_token(response):
        continuation_token = response.get('NextContinuationToken')
        if not continuation_token:
            raise RuntimeError('S3 response is truncated without continuation token')
        return continuation_token

    def get_project_storage_usage(self, project_uuid):
        project_uuid_36 = uuid_str_to_36_chars(project_uuid)

        file_size, file_object_count = self._get_s3_prefix_storage_usage(
            self.file_bucket, f'/projects/{project_uuid_36}/'
        )
        crawl_size, crawl_object_count = self._get_s3_prefix_storage_usage(
            self.bucket, f'{project_uuid}/'
        )
        return {
            'file_size': file_size,
            'file_object_count': file_object_count,
            'crawl_data_size': crawl_size,
            'crawl_object_count': crawl_object_count,
        }

    def delete_connection_crawl_data(self, connection_id, project_uuid):
        prefix = KEY_PREFIX_FORMAT.format(project_uuid=project_uuid, site_id=connection_id)
        self._delete_by_prefix(self.bucket, prefix)

    def delete_project_crawl_data(self, project_uuid):
        prefix = f"{project_uuid}/"
        self._delete_by_prefix(self.bucket, prefix)

    def delete_project_attachment(self, project_uuid):
        project_uuid = uuid_str_to_36_chars(project_uuid)
        self._delete_by_prefix(self.file_bucket, f'/projects/{project_uuid}/')


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
