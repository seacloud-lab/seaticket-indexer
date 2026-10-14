import os
import shutil
import stat
import uuid
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from seaqa_indexer.config import (
    ATTACHMENT_STORAGE_BACKEND,
    ATTACHMENT_STORAGE_LOCAL_ROOT,
    S3_FILE_BUCKET,
    S3_HOST,
    S3_KEY_ID,
    S3_SECRET_KEY,
    S3_WEB_CRAWL_BUCKET,
)

PROJECT_ATTACHMENTS = 'project_attachments'
CONNECTION_ATTACHMENTS = 'connection_attachments'


class FileNotFound(Exception):
    pass


def _validate_directory_prefix(prefix):
    if not isinstance(prefix, str) or not prefix.endswith('/'):
        raise ValueError('Attachment prefix must end with "/"')


class BaseBackend:
    def get_file_by_path(self, attachment_type, path):
        raise NotImplementedError

    def upload_file_by_path(self, attachment_type, source_path, path, extra_args=None):
        raise NotImplementedError

    def upload_file_by_bytes(self, attachment_type, path, content):
        raise NotImplementedError

    def delete_file(self, attachment_type, path):
        raise NotImplementedError

    def delete_files_by_prefix(self, attachment_type, prefix):
        raise NotImplementedError

    def get_file_metadata(self, attachment_type, path):
        raise NotImplementedError

    def list_files_by_prefix(self, attachment_type, prefix):
        raise NotImplementedError

class S3Backend(BaseBackend):
    def __init__(self, endpoint_url, access_key, secret_key, project_bucket, connection_bucket):
        self.client = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            endpoint_url=endpoint_url,
            config=BotoConfig(signature_version='s3v4', s3={'addressing_style': 'path'}),
        )
        self.buckets = {
            PROJECT_ATTACHMENTS: project_bucket,
            CONNECTION_ATTACHMENTS: connection_bucket,
        }

    def _bucket(self, attachment_type):
        try:
            bucket = self.buckets[attachment_type]
        except KeyError as e:
            raise ValueError('Unknown attachment type: %s' % attachment_type) from e
        if not bucket:
            raise ValueError('S3 bucket is not configured for %s' % attachment_type)
        return bucket

    def get_file_by_path(self, attachment_type, path):
        try:
            return self.client.get_object(Bucket=self._bucket(attachment_type), Key=path)['Body']
        except ClientError as e:
            if e.response.get('Error', {}).get('Code') in ('NoSuchKey', '404', 'NotFound'):
                raise FileNotFound(path) from e
            raise

    def upload_file_by_path(self, attachment_type, source_path, path, extra_args=None):
        self.client.upload_file(source_path, self._bucket(attachment_type), path, ExtraArgs=extra_args or {})

    def upload_file_by_bytes(self, attachment_type, path, content):
        self.client.put_object(Bucket=self._bucket(attachment_type), Key=path, Body=content)

    def delete_file(self, attachment_type, path):
        self.client.delete_object(Bucket=self._bucket(attachment_type), Key=path)

    def delete_files_by_prefix(self, attachment_type, prefix):
        _validate_directory_prefix(prefix)
        bucket = self._bucket(attachment_type)
        continuation_token = None
        while True:
            params = {'Bucket': bucket, 'Prefix': prefix}
            if continuation_token:
                params['ContinuationToken'] = continuation_token
            response = self.client.list_objects_v2(**params)
            objects = [{'Key': item['Key']} for item in response.get('Contents') or []]
            for start in range(0, len(objects), 1000):
                result = self.client.delete_objects(
                    Bucket=bucket,
                    Delete={'Objects': objects[start:start + 1000]},
                )
                errors = result.get('Errors') or []
                if errors:
                    codes = sorted({error.get('Code', 'Unknown') for error in errors})
                    raise RuntimeError('Failed to delete S3 objects: %s' % ', '.join(codes))
            if not response.get('IsTruncated'):
                break
            continuation_token = response.get('NextContinuationToken')
            if not continuation_token:
                raise RuntimeError('S3 response is truncated without continuation token')

    def get_file_metadata(self, attachment_type, path):
        try:
            return self.client.head_object(Bucket=self._bucket(attachment_type), Key=path)
        except ClientError as e:
            if e.response.get('Error', {}).get('Code') in ('NoSuchKey', '404', 'NotFound'):
                raise FileNotFound(path) from e
            raise

    def list_files_by_prefix(self, attachment_type, prefix):
        _validate_directory_prefix(prefix)
        bucket = self._bucket(attachment_type)
        continuation_token = None
        while True:
            params = {'Bucket': bucket, 'Prefix': prefix}
            if continuation_token:
                params['ContinuationToken'] = continuation_token
            response = self.client.list_objects_v2(**params)
            for item in response.get('Contents') or []:
                yield item['Key'], item['Size']
            if not response.get('IsTruncated'):
                break
            continuation_token = response.get('NextContinuationToken')
            if not continuation_token:
                raise RuntimeError('S3 response is truncated without continuation token')


class LocalBackend(BaseBackend):
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, attachment_type, path):
        if attachment_type not in (PROJECT_ATTACHMENTS, CONNECTION_ATTACHMENTS):
            raise ValueError('Unknown attachment type: %s' % attachment_type)
        relative_path = Path(path.lstrip('/'))
        if relative_path.is_absolute() or '..' in relative_path.parts:
            raise ValueError('Invalid attachment path')
        base = (self.root / attachment_type).resolve()
        target = (base / relative_path).resolve()
        if not base.is_relative_to(self.root) or not target.is_relative_to(base):
            raise ValueError('Attachment path escapes storage root')
        return target

    def get_file_by_path(self, attachment_type, path):
        target = self._path(attachment_type, path)
        try:
            return target.open('rb')
        except FileNotFoundError as e:
            raise FileNotFound(path) from e

    @staticmethod
    def _write_atomically(target, writer):
        while True:
            temp_path = target.with_name(f'.attachment-upload-{uuid.uuid4().hex}.tmp')
            try:
                fd = os.open(temp_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o666)
                break
            except FileExistsError:
                continue
        try:
            try:
                existing_mode = stat.S_IMODE(target.stat().st_mode)
            except FileNotFoundError:
                existing_mode = None
            if existing_mode is not None:
                os.fchmod(fd, existing_mode)
            with os.fdopen(fd, 'wb') as output:
                writer(output)
            os.replace(temp_path, target)
        finally:
            try:
                os.unlink(temp_path)
            except FileNotFoundError:
                pass

    def upload_file_by_path(self, attachment_type, source_path, path, extra_args=None):
        target = self._path(attachment_type, path)
        target.parent.mkdir(parents=True, exist_ok=True)
        with open(source_path, 'rb') as source:
            self._write_atomically(target, lambda output: shutil.copyfileobj(source, output))

    def upload_file_by_bytes(self, attachment_type, path, content):
        target = self._path(attachment_type, path)
        target.parent.mkdir(parents=True, exist_ok=True)
        self._write_atomically(target, lambda output: output.write(content))

    def delete_file(self, attachment_type, path):
        target = self._path(attachment_type, path)
        try:
            target.unlink()
        except FileNotFoundError:
            pass

    def delete_files_by_prefix(self, attachment_type, prefix):
        _validate_directory_prefix(prefix)
        target = self._path(attachment_type, prefix)
        base = (self.root / attachment_type).resolve()
        if not target.is_dir():
            return
        cleanup_paths = {target}
        for candidate in target.rglob('*'):
            if candidate.is_file():
                candidate.unlink()
                cleanup_paths.add(candidate.parent)

        for path in sorted(cleanup_paths, key=lambda item: len(item.parts), reverse=True):
            while path != base and path.is_relative_to(base):
                try:
                    path.rmdir()
                except OSError:
                    break
                path = path.parent

    def get_file_metadata(self, attachment_type, path):
        target = self._path(attachment_type, path)
        try:
            stat = target.stat()
        except FileNotFoundError as e:
            raise FileNotFound(path) from e
        if not target.is_file():
            raise FileNotFound(path)
        last_modified_timestamp = stat.st_mtime
        return {
            'ETag': f'W/"{stat.st_size}-{stat.st_mtime_ns}"',
            'LastModified': datetime.fromtimestamp(last_modified_timestamp, timezone.utc),
            'ContentLength': stat.st_size,
        }

    def list_files_by_prefix(self, attachment_type, prefix):
        _validate_directory_prefix(prefix)
        target = self._path(attachment_type, prefix)
        if not target.is_dir():
            return
        for path in target.rglob('*'):
            is_upload_temp = (
                path.name.startswith('.attachment-upload-') and path.name.endswith('.tmp')
            )
            if not path.is_file() or is_upload_temp:
                continue
            try:
                size = path.stat().st_size
            except FileNotFoundError:
                continue
            yield path.relative_to(self.root / attachment_type).as_posix(), size

_backend = None


def get_backend():
    global _backend
    if _backend is not None:
        return _backend
    if ATTACHMENT_STORAGE_BACKEND == 's3':
        _backend = S3Backend(
            S3_HOST,
            S3_KEY_ID,
            S3_SECRET_KEY,
            S3_FILE_BUCKET,
            S3_WEB_CRAWL_BUCKET,
        )
    elif ATTACHMENT_STORAGE_BACKEND == 'local':
        if not ATTACHMENT_STORAGE_LOCAL_ROOT:
            raise ValueError('ATTACHMENT_STORAGE_LOCAL_ROOT must be configured for local storage')
        _backend = LocalBackend(ATTACHMENT_STORAGE_LOCAL_ROOT)
    else:
        raise ValueError('Unsupported ATTACHMENT_STORAGE_BACKEND: %s' % ATTACHMENT_STORAGE_BACKEND)
    return _backend
