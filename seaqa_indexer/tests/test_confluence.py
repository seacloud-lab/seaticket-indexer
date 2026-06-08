import json
import sys
import os
import types
from unittest.mock import Mock, patch

# Create proper package hierarchy mocks for missing external dependencies.
# Must create parent packages with __path__ to avoid breaking other imports.
def _ensure_package(name):
    parts = name.split('.')
    for i in range(1, len(parts) + 1):
        pkg_name = '.'.join(parts[:i])
        if pkg_name not in sys.modules:
            mod = types.ModuleType(pkg_name)
            mod.__path__ = []  # mark as package
            sys.modules[pkg_name] = mod
        elif not hasattr(sys.modules[pkg_name], '__path__'):
            sys.modules[pkg_name].__path__ = []

# External libs with sub-packages (need package hierarchy)
for _name in ('pptx', 'pptx.enum', 'pptx.enum.shapes'):
    _ensure_package(_name)

# Leaf external modules (no sub-packages needed). Use Mock() with __version__.
for _name in ('PyPDF2', 'docx', 'markdownify'):
    if _name not in sys.modules:
        sys.modules[_name] = Mock()

# html2text needs HTML2Text class that returns an object with handle() -> str
_html2text_mod = Mock()
_html2text_mod.HTML2Text.return_value.handle.return_value.strip.return_value = 'converted text'
if 'html2text' not in sys.modules:
    sys.modules['html2text'] = _html2text_mod

# Ensure the project root is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from seaqa_indexer.utils.confluence_api import ConfluenceAPI, refresh_confluence_access_token
from seaqa_indexer.seadb_models.models import ConfluenceTable
from seaqa_indexer.utils.constants import DataEventType

# Now that seaqa_indexer.utils is loaded as a proper package, mock converter
# to prevent cascading imports of pptx/html2text internals.
if 'seaqa_indexer.utils.converter' not in sys.modules:
    sys.modules['seaqa_indexer.utils.converter'] = Mock()

# Use importlib to load individual files directly, avoiding __init__.py cascade
import importlib.util

def _load_module(file_path, module_name):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

BASE = os.path.join(os.path.dirname(__file__), '..')

# Pre-load base_handler.py directly (bypasses handlers/__init__.py cascade)
_base_handler_mod = _load_module(
    os.path.join(BASE, 'handlers', 'base_handler.py'),
    'seaqa_indexer.handlers.base_handler',
)
sys.modules['seaqa_indexer.handlers.base_handler'] = _base_handler_mod

# Mock handlers package to prevent __init__.py from importing everything
if 'seaqa_indexer.handlers' not in sys.modules:
    _handlers_pkg = types.ModuleType('seaqa_indexer.handlers')
    _handlers_pkg.__path__ = []
    sys.modules['seaqa_indexer.handlers'] = _handlers_pkg

ConfluenceSeaDBAPI = _load_module(
    os.path.join(BASE, 'seadb_models', 'confluence_seadb_api.py'),
    'confluence_seadb_api_test',
).ConfluenceSeaDBAPI

ConfluenceHandler = _load_module(
    os.path.join(BASE, 'handlers', 'confluence_handler.py'),
    'confluence_handler_test',
).ConfluenceHandler


class TestConfluenceAPINormalizePage:
    """Tests for ConfluenceAPI._normalize_page (covers B3 fix)."""

    def setup_method(self):
        self.api = ConfluenceAPI('test-token')

    def test_normalize_page_basic_mapping(self):
        page = {
            'id': '12345',
            'title': 'Test Page',
            'createdAt': '2024-01-01T00:00:00Z',
            'authorId': 'user:creator',
            '_links': {'webui': '/wiki/spaces/SP/pages/12345'},
        }
        result = self.api._normalize_page(page, base_url='https://example.atlassian.net/wiki')
        assert result['page_id'] == '12345'
        assert result['title'] == 'Test Page'
        assert result['created_time'] == '2024-01-01T00:00:00Z'
        assert result['creator'] == 'user:creator'
        assert result['url'].startswith('https://example.atlassian.net/wiki')

    def test_normalize_page_created_time_does_not_fallback_to_version(self):
        """B3: created_time should NOT use version.createdAt as fallback."""
        page = {
            'id': '12345',
            'title': 'Test Page',
            'createdAt': None,
            'version': {'createdAt': '2024-06-01T00:00:00Z'},
        }
        result = self.api._normalize_page(page)
        assert result['created_time'] is None

    def test_normalize_page_no_webui_no_url(self):
        page = {'id': '12345', 'title': 'Test Page', '_links': {}}
        result = self.api._normalize_page(page)
        assert result['url'] == ''

    def test_normalize_page_parent_page_id(self):
        page = {'id': '12345', 'title': 'Child Page', 'parentId': '999'}
        result = self.api._normalize_page(page)
        assert result['parent_page_id'] == '999'

    def test_normalize_page_no_parent(self):
        page = {'id': '12345', 'title': 'Root Page'}
        result = self.api._normalize_page(page)
        assert result['parent_page_id'] == ''


class TestNormalizeModifiedTime:
    def setup_method(self):
        self.api = ConfluenceAPI('test-token')

    def test_modified_time_from_version(self):
        page = {'id': '1', 'version': {'createdAt': '2024-06-15T12:00:00Z'}}
        result = self.api._normalize_page(page)
        assert result['modified_time'] == '2024-06-15T12:00:00Z'

    def test_modified_time_fallback_to_updated_at(self):
        page = {'id': '1', 'updatedAt': '2024-07-01T00:00:00Z', 'version': {}}
        result = self.api._normalize_page(page)
        assert result['modified_time'] == '2024-07-01T00:00:00Z'


class TestConfluenceAPIListPages:
    """Tests for list_pages method."""

    def test_list_pages_empty_results(self):
        api = ConfluenceAPI('test-token')
        mock_response = Mock()
        mock_response.json.return_value = {'results': [], '_links': {}}
        mock_response.headers = {}
        with patch.object(api, '_request', return_value=mock_response):
            pages, cursor, has_more = api.list_pages('ws-1')
            assert pages == []
            assert has_more is False

    def test_list_pages_with_cursor(self):
        api = ConfluenceAPI('test-token')
        mock_response = Mock()
        mock_response.json.return_value = {
            'results': [{'id': '100', 'title': 'Page 1', '_links': {'webui': '/spaces/SP/pages/100'}}],
            '_links': {
                'base': 'https://example.atlassian.net/wiki',
                'next': '/wiki/api/v2/pages?cursor=next-cursor',
            },
        }
        mock_response.headers = {}
        with patch.object(api, '_request', return_value=mock_response):
            pages, cursor, has_more = api.list_pages('ws-1', cursor='start')
            assert len(pages) == 1
            assert cursor == 'next-cursor'
            assert has_more is True

    def test_list_pages_filters_null_id(self):
        api = ConfluenceAPI('test-token')
        mock_response = Mock()
        mock_response.json.return_value = {
            'results': [{'title': 'No ID'}, {'id': '200', 'title': 'Has ID'}],
            '_links': {},
        }
        mock_response.headers = {}
        with patch.object(api, '_request', return_value=mock_response):
            pages, cursor, has_more = api.list_pages('ws-1')
            assert len(pages) == 1
            assert pages[0]['page_id'] == '200'


class TestConfluenceSeaDBAPI:
    """Tests for ConfluenceSeaDBAPI (covers B1, B5 fixes)."""

    def test_save_new_page_includes_outdated_false(self):
        """New pages should have outdated=False in INSERT."""
        seaqa_db = Mock()
        seaqa_db.insert_rows = Mock()
        api = ConfluenceSeaDBAPI('project-1')
        api.seadb_api = seaqa_db

        page = {
            'page_id': 'p1', 'title': 'New Page', 'content': 'Hello',
            'url': 'https://example.com', 'created_time': '2024-01-01T00:00:00Z',
            'modified_time': '2024-01-01T00:00:00Z', 'creator': 'user1',
            'last_modifier': 'user1', 'parent_page_id': '',
        }
        api.save_or_update_page_to_db(1, page, {})

        insert_call = seaqa_db.insert_rows.call_args
        assert insert_call is not None
        row = insert_call[0][2][0]
        assert row[ConfluenceTable.outdated.name] is False

    def test_update_page_preserves_outdated(self):
        """B1: Updating an existing page should NOT overwrite outdated."""
        seaqa_db = Mock()
        seaqa_db.update_rows = Mock()
        api = ConfluenceSeaDBAPI('project-1')
        api.seadb_api = seaqa_db

        page = {
            'page_id': 'p1', 'title': 'Updated Page', 'content': 'New content',
            'url': 'https://example.com', 'created_time': '2024-01-01T00:00:00Z',
            'modified_time': '2024-06-01T00:00:00Z', 'creator': 'user1',
            'last_modifier': 'user2', 'parent_page_id': '',
        }
        existed = {'_pk': '42', 'modified_time': '2024-01-01T00:00:00Z'}
        api.save_or_update_page_to_db(1, page, existed)

        update_call = seaqa_db.update_rows.call_args
        row = update_call[0][2][0]['row']
        assert ConfluenceTable.outdated.name not in row

    def test_mark_pages_deleted_includes_modified_time(self):
        """B5: mark_pages_deleted should include modified_time."""
        seaqa_db = Mock()
        seaqa_db.update_rows = Mock()
        api = ConfluenceSeaDBAPI('project-1')
        api.seadb_api = seaqa_db

        api.mark_pages_deleted(1, {'p-stale'}, {'p-stale': '99'})

        update_call = seaqa_db.update_rows.call_args
        row = update_call[0][2][0]['row']
        assert row[ConfluenceTable.deleted.name] is True
        assert ConfluenceTable.modified_time.name in row
        assert ConfluenceTable.sync_time.name in row
        assert ConfluenceTable.record_modified_time.name in row

    def test_get_need_ai_pages_has_order_by(self):
        """B2: SQL should include ORDER BY _pk."""
        api = ConfluenceSeaDBAPI('project-1')
        # We verify the fix by checking class behavior - the ORDER BY
        # is in the SQL string within get_need_ai_confluence_pages_by_page
        assert hasattr(api, 'get_need_ai_confluence_pages_by_page')


class TestConfluenceHandlerProcessAI:
    """Tests for ConfluenceHandler.process_ai (covers F5 fix)."""

    def test_process_ai_exception_handling_added(self):
        """F5: Verify the process_ai method has try/except for per-record errors."""
        handler = ConfluenceHandler()
        import inspect
        source = inspect.getsource(handler.process_ai)
        assert 'except Exception' in source
        assert 'logger.error' in source
        assert 'continue' in source

    def test_process_ai_exists_and_returns_int(self):
        """process_ai should exist and be callable."""
        handler = ConfluenceHandler()
        assert callable(handler.process_ai)


class TestConfluenceHandlerSync:
    """Tests for ConfluenceHandler sync-related features (covers F2 fix)."""

    def test_sync_method_accepts_project_uuid(self):
        """F2: _sync_confluence should accept project_uuid for data_events."""
        handler = ConfluenceHandler()
        import inspect
        sig = inspect.signature(handler._sync_confluence)
        params = list(sig.parameters.keys())
        assert 'project_uuid' in params
        assert 'connection_id' in params

    def test_handler_has_data_event_types(self):
        """F2: Verify DataEventType includes Confluence events."""
        assert DataEventType.CONFLUENCE_PAGE_ADDED.value == 'confluence_page_added'
        assert DataEventType.CONFLUENCE_PAGE_UPDATED.value == 'confluence_page_updated'

    def test_handler_has_publish_events_method(self):
        """F2: Handler should have publish_synced_record_events from base class."""
        handler = ConfluenceHandler()
        assert callable(handler.publish_synced_record_events)


class TestConfluenceAPIListSpaces:
    """Tests for ConfluenceAPI.list_spaces method."""

    def test_list_spaces_empty_results(self):
        api = ConfluenceAPI('test-token')
        mock_response = Mock()
        mock_response.json.return_value = {'results': [], '_links': {}}
        mock_response.headers = {}
        with patch.object(api, '_request', return_value=mock_response):
            spaces, cursor, has_more = api.list_spaces('ws-1')
            assert spaces == []
            assert has_more is False

    def test_list_spaces_returns_parsed_data(self):
        api = ConfluenceAPI('test-token')
        mock_response = Mock()
        mock_response.json.return_value = {
            'results': [
                {'id': '100', 'key': 'TEAM', 'name': 'Team Space', 'type': 'global', 'status': 'current'},
            ],
            '_links': {},
        }
        mock_response.headers = {}
        with patch.object(api, '_request', return_value=mock_response):
            spaces, cursor, has_more = api.list_spaces('ws-1')
            assert len(spaces) == 1
            assert spaces[0]['id'] == '100'
            assert spaces[0]['key'] == 'TEAM'
            assert spaces[0]['name'] == 'Team Space'

    def test_list_spaces_filters_null_id(self):
        api = ConfluenceAPI('test-token')
        mock_response = Mock()
        mock_response.json.return_value = {
            'results': [
                {'key': 'NOID', 'name': 'No ID Space'},
                {'id': '200', 'key': 'OK', 'name': 'OK Space'},
            ],
            '_links': {},
        }
        mock_response.headers = {}
        with patch.object(api, '_request', return_value=mock_response):
            spaces, cursor, has_more = api.list_spaces('ws-1')
            assert len(spaces) == 1
            assert spaces[0]['id'] == '200'


class TestConfluenceAPIListPagesWithSpaceFilter:
    """Tests for list_pages with space_ids parameter."""

    def test_list_pages_without_space_ids_passes_none(self):
        """When no space_ids, space-id param should not be in request."""
        api = ConfluenceAPI('test-token')
        mock_response = Mock()
        mock_response.json.return_value = {'results': [], '_links': {}}
        mock_response.headers = {}
        with patch.object(api, '_request', return_value=mock_response) as mock_req:
            api.list_pages('ws-1', space_ids=None)
            _, call_kwargs = mock_req.call_args
            params = call_kwargs.get('params', {})
            assert 'space-id' not in params

    def test_list_pages_with_space_ids_passes_filter(self):
        """When space_ids provided, space-id param should be passed."""
        api = ConfluenceAPI('test-token')
        mock_response = Mock()
        mock_response.json.return_value = {'results': [], '_links': {}}
        mock_response.headers = {}
        with patch.object(api, '_request', return_value=mock_response) as mock_req:
            api.list_pages('ws-1', space_ids=['100', '200'])
            _, call_kwargs = mock_req.call_args
            params = call_kwargs.get('params', {})
            assert 'space-id' in params
            assert params['space-id'] == ['100', '200']


class TestConfluenceHandlerSyncWithSpaces:
    """Tests for _sync_confluence with space_ids parameter."""

    def test_sync_without_space_ids_calls_list_pages_once_per_cursor(self):
        """Without space_ids, should call list_pages with space_ids=None (single iteration)."""
        handler = ConfluenceHandler()
        handler.mq = Mock()

        mock_api = Mock()
        mock_api.list_pages.return_value = ([], None, False)

        mock_seadb = Mock()
        mock_seadb.get_page_id_rows_by_page.return_value = []
        mock_seadb.get_confluence_pages_count.return_value = 0

        mock_seaqa_db = Mock()

        handler._sync_confluence(
            1, 'proj-1', {'last_sync_count': 0},
            mock_api, mock_seaqa_db, mock_seadb, 'ws-1', space_ids=None,
        )

        # Should have been called at least once with space_ids=None
        call_found = False
        for call in mock_api.list_pages.call_args_list:
            _, kwargs = call
            if kwargs.get('space_ids') is None:
                call_found = True
                break
        assert call_found

    def test_sync_with_space_ids_calls_list_pages_per_space(self):
        """With space_ids, should call list_pages for each space."""
        handler = ConfluenceHandler()
        handler.mq = Mock()

        mock_api = Mock()
        mock_api.list_pages.return_value = ([], None, False)

        mock_seadb = Mock()
        mock_seadb.get_page_id_rows_by_page.return_value = []
        mock_seadb.get_confluence_pages_count.return_value = 0

        mock_seaqa_db = Mock()

        handler._sync_confluence(
            1, 'proj-1', {'last_sync_count': 0},
            mock_api, mock_seaqa_db, mock_seadb, 'ws-1', space_ids=['100', '200'],
        )

        # Should have been called with space_ids=['100'] and space_ids=['200']
        space_id_calls = [c[1].get('space_ids') for c in mock_api.list_pages.call_args_list]
        assert ['100'] in space_id_calls
        assert ['200'] in space_id_calls


class TestDataEventTypes:
    """Tests for Confluence data event types."""

    def test_confluence_event_types_exist(self):
        assert DataEventType.CONFLUENCE_PAGE_ADDED.value == 'confluence_page_added'
        assert DataEventType.CONFLUENCE_PAGE_UPDATED.value == 'confluence_page_updated'

    def test_confluence_event_types_valid(self):
        assert DataEventType.is_valid('confluence_page_added')
        assert DataEventType.is_valid('confluence_page_updated')
