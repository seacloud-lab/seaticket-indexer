import pytest
from unittest.mock import patch
from pathlib import Path

from seaqa_indexer.utils.attachment_backend import (
    CONNECTION_ATTACHMENTS,
    PROJECT_ATTACHMENTS,
    FileNotFound,
    LocalBackend,
    S3Backend,
    get_backend,
)
from seaqa_indexer.syncer.utils import get_project_storage_usage


def test_backend_factory_loads_disk_configuration(monkeypatch, tmp_path):
    monkeypatch.setattr('seaqa_indexer.utils.attachment_backend.ATTACHMENT_STORAGE_BACKEND', 'disk')
    monkeypatch.setattr('seaqa_indexer.utils.attachment_backend.ATTACHMENT_STORAGE_LOCAL_ROOT', str(tmp_path))
    monkeypatch.setattr('seaqa_indexer.utils.attachment_backend._backend', None)

    backend = get_backend()

    assert isinstance(backend, LocalBackend)
    assert backend.root == tmp_path.resolve()


def test_backend_factory_rejects_local_configuration(monkeypatch):
    monkeypatch.setattr('seaqa_indexer.utils.attachment_backend.ATTACHMENT_STORAGE_BACKEND', 'local')
    monkeypatch.setattr('seaqa_indexer.utils.attachment_backend._backend', None)

    with pytest.raises(ValueError, match='Unsupported ATTACHMENT_STORAGE_BACKEND: local'):
        get_backend()


def test_local_backend_round_trips_project_and_connection_files(tmp_path):
    backend = LocalBackend(tmp_path)
    project_path = '/projects/project-id/ticket/1/image.png'
    connection_path = 'projectid/12/34/report.txt'

    backend.upload_file_by_bytes(PROJECT_ATTACHMENTS, project_path, b'image')
    backend.upload_file_by_bytes(CONNECTION_ATTACHMENTS, connection_path, b'attachment')

    with backend.get_file_by_path(PROJECT_ATTACHMENTS, project_path) as stored_file:
        assert stored_file.read() == b'image'
    with backend.get_file_by_path(CONNECTION_ATTACHMENTS, connection_path) as stored_file:
        assert stored_file.read() == b'attachment'
    metadata = backend.get_file_metadata(PROJECT_ATTACHMENTS, project_path)
    assert metadata['ContentLength'] == 5
    assert metadata['ETag'].startswith('W/')
    assert metadata['LastModified'].tzinfo is not None


def test_local_backend_failed_atomic_write_preserves_existing_file(tmp_path):
    backend = LocalBackend(tmp_path)
    target = backend._path(PROJECT_ATTACHMENTS, 'project/file.txt')
    target.parent.mkdir(parents=True)
    target.write_bytes(b'old-complete-content')

    def fail_after_partial_write(output):
        output.write(b'partial')
        raise OSError('simulated disk failure')

    with pytest.raises(OSError, match='simulated disk failure'):
        backend._write_atomically(target, fail_after_partial_write)

    assert target.read_bytes() == b'old-complete-content'
    assert list(target.parent.iterdir()) == [target]


def test_local_backend_atomic_replacement_preserves_existing_permissions(tmp_path):
    backend = LocalBackend(tmp_path)
    target = backend._path(PROJECT_ATTACHMENTS, 'project/shared.txt')
    target.parent.mkdir(parents=True)
    target.write_bytes(b'old')
    target.chmod(0o640)

    backend.upload_file_by_bytes(PROJECT_ATTACHMENTS, 'project/shared.txt', b'new-content')

    assert target.read_bytes() == b'new-content'
    assert target.stat().st_mode & 0o777 == 0o640


def test_local_backend_supports_maximum_component_length_filename(tmp_path):
    backend = LocalBackend(tmp_path)
    filename = 'x' * 255
    path = f'project/{filename}'

    backend.upload_file_by_bytes(PROJECT_ATTACHMENTS, path, b'payload')

    with backend.get_file_by_path(PROJECT_ATTACHMENTS, path) as stored_file:
        assert stored_file.read() == b'payload'


def test_local_backend_missing_file_raises_storage_not_found(tmp_path):
    backend = LocalBackend(tmp_path)

    with pytest.raises(FileNotFound):
        backend.get_file_by_path(PROJECT_ATTACHMENTS, '/projects/missing/file.txt')


def test_local_backend_rejects_path_traversal(tmp_path):
    backend = LocalBackend(tmp_path)

    with pytest.raises(ValueError):
        backend.upload_file_by_bytes(PROJECT_ATTACHMENTS, '../outside.txt', b'data')


def test_local_backend_prefix_delete_is_exact_and_reports_usage(tmp_path):
    backend = LocalBackend(tmp_path)
    project_uuid = 'd5439489-198f-4f54-9bb2-0261dd582ea3'
    backend.upload_file_by_bytes(PROJECT_ATTACHMENTS, f'/projects/{project_uuid}/attachments/ticket/1/a', b'1')
    backend.upload_file_by_bytes(PROJECT_ATTACHMENTS, f'/projects/{project_uuid}/attachments/ticket/10/a', b'10')
    backend.upload_file_by_bytes(CONNECTION_ATTACHMENTS, 'd5439489198f4f549bb20261dd582ea3/2/3/a', b'mail')

    backend.delete_files_by_prefix(PROJECT_ATTACHMENTS, f'/projects/{project_uuid}/attachments/ticket/1/')
    usage = get_project_storage_usage(backend, project_uuid)

    assert (tmp_path / f'project_attachments/projects/{project_uuid}/attachments/ticket/1/a').exists() is False
    assert (tmp_path / f'project_attachments/projects/{project_uuid}/attachments/ticket/10/a').read_bytes() == b'10'
    assert usage['crawl_data_size'] == 4
    assert usage['crawl_object_count'] == 1


def test_local_backend_prefix_delete_only_scans_and_cleans_matching_tree(tmp_path):
    backend = LocalBackend(tmp_path)
    target = tmp_path / PROJECT_ATTACHMENTS / 'project' / 'connection' / 'email'
    target.mkdir(parents=True)
    (target / 'message.txt').write_text('remove')
    unrelated = tmp_path / PROJECT_ATTACHMENTS / 'other-project' / 'empty'
    unrelated.mkdir(parents=True)

    backend.delete_files_by_prefix(PROJECT_ATTACHMENTS, 'project/connection/')

    assert not target.exists()
    assert unrelated.is_dir()


def test_local_backend_prefix_operations_require_directory_prefix(tmp_path):
    backend = LocalBackend(tmp_path)

    with pytest.raises(ValueError, match='must end with'):
        backend.delete_files_by_prefix(PROJECT_ATTACHMENTS, 'project/12')
    with pytest.raises(ValueError, match='must end with'):
        list(backend.list_files_by_prefix(PROJECT_ATTACHMENTS, 'project/12'))


def test_local_backend_list_files_by_prefix_only_walks_requested_directory(tmp_path):
    backend = LocalBackend(tmp_path)
    backend.upload_file_by_bytes(PROJECT_ATTACHMENTS, 'project/12/a.txt', b'a')
    backend.upload_file_by_bytes(PROJECT_ATTACHMENTS, 'project/123/b.txt', b'b')

    listed = list(backend.list_files_by_prefix(PROJECT_ATTACHMENTS, 'project/12/'))

    assert listed == [('project/12/a.txt', 1)]


def test_local_backend_list_files_ignores_atomic_upload_temps(tmp_path):
    backend = LocalBackend(tmp_path)
    directory = tmp_path / PROJECT_ATTACHMENTS / 'project'
    directory.mkdir(parents=True)
    (directory / '.attachment-upload-pending.tmp').write_bytes(b'unpublished')
    (directory / 'published.txt').write_bytes(b'published')

    assert list(backend.list_files_by_prefix(PROJECT_ATTACHMENTS, 'project/')) == [
        ('project/published.txt', len(b'published')),
    ]


def test_local_backend_lists_regular_files_using_temp_prefix(tmp_path):
    backend = LocalBackend(tmp_path)
    backend.upload_file_by_bytes(
        PROJECT_ATTACHMENTS, 'project/.attachment-upload-user.md', b'published'
    )

    assert list(backend.list_files_by_prefix(PROJECT_ATTACHMENTS, 'project/')) == [
        ('project/.attachment-upload-user.md', len(b'published')),
    ]


def test_local_backend_list_files_skips_paths_disappearing_before_stat(tmp_path, monkeypatch):
    backend = LocalBackend(tmp_path)
    backend.upload_file_by_bytes(PROJECT_ATTACHMENTS, 'project/file.txt', b'content')
    target = tmp_path / PROJECT_ATTACHMENTS / 'project' / 'file.txt'
    original_stat = Path.stat

    def stat_then_remove(path, *args, **kwargs):
        result = original_stat(path, *args, **kwargs)
        if path == target:
            path.unlink(missing_ok=True)
        return result

    monkeypatch.setattr(Path, 'stat', stat_then_remove)

    assert list(backend.list_files_by_prefix(PROJECT_ATTACHMENTS, 'project/')) == []


def test_s3_backend_receives_configuration_through_constructor():
    with patch('seaqa_indexer.utils.attachment_backend.boto3.client') as client_factory:
        backend = S3Backend('https://s3.example', 'access', 'secret', 'files', 'connections')

    client_factory.assert_called_once()
    assert backend.buckets == {
        PROJECT_ATTACHMENTS: 'files',
        CONNECTION_ATTACHMENTS: 'connections',
    }
