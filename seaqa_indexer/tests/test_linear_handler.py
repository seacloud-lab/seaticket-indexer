import json
from unittest.mock import Mock

from seaqa_indexer.handlers.linear_handler import LinearIssueHandler
from seaqa_indexer.seadb_models.linear_seadb_api import LinearSeaDBAPI
from seaqa_indexer.seadb_models.models import SchemaTables
from seaqa_indexer.utils.constants import ConnectionType


def test_upsert_user_inserts_new_user():
    api = LinearSeaDBAPI('project')
    api.seadb_api = Mock()
    api.seadb_api.query_rows.return_value = {'results': []}

    api.upsert_user(12, 'user-1', 'Alice')

    row = api.seadb_api.insert_rows.call_args.args[2][0]
    assert row[SchemaTables.CONNECTION_USER.column.user_id.name] == 'user-1'
    assert row[SchemaTables.CONNECTION_USER.column.name.name] == 'Alice'
    assert row[SchemaTables.CONNECTION_USER.column.connection_ids.name] == [12]
    assert row[SchemaTables.CONNECTION_USER.column.connection_type.name] == ConnectionType.LINEAR.value


def test_upsert_user_appends_connection_id():
    api = LinearSeaDBAPI('project')
    api.seadb_api = Mock()
    api.seadb_api.query_rows.return_value = {'results': [{'_pk': 1, 'connection_ids': [10]}]}

    api.upsert_user(12, 'user-1', 'Alice')

    update = api.seadb_api.update_rows.call_args.args[2][0]
    assert update['pk'] == 1
    assert update['row'][SchemaTables.CONNECTION_USER.column.connection_ids.name] == [10, 12]
    assert update['row'][SchemaTables.CONNECTION_USER.column.name.name] == 'Alice'


def test_sync_linear_issue_upserts_creator_assignee_and_comment_users():
    handler = LinearIssueHandler()
    linear_api = Mock()
    linear_api.list_issues.return_value = ([
        {
            'id': 'issue-1',
            'updatedAt': '2026-01-02T00:00:00Z',
            'creator': {'id': 'u1', 'name': 'Alice'},
            'assignee': {'id': 'u2', 'name': 'Bob'},
        }
    ], {'hasNextPage': False})
    linear_api.list_issue_comments.return_value = ([
        {'id': 'c1', 'user': {'id': 'u3', 'name': 'Carol'}},
        {'id': 'c2', 'user': {'id': 'u1', 'name': 'Alice'}},
        {'id': 'c3', 'user': {'name': 'NoId'}},
    ], {'hasNextPage': False})

    linear_seadb_api = Mock()
    linear_seadb_api.get_issue_by_issue_id.return_value = {}
    linear_seadb_api.get_linear_issues_count.return_value = 1

    handler._sync_linear_issue(
        12, None, {}, linear_api, Mock(), linear_seadb_api, 'team-1', True, False
    )

    assert linear_seadb_api.upsert_user.call_count == 3
    linear_seadb_api.upsert_user.assert_any_call(12, 'u1', 'Alice')
    linear_seadb_api.upsert_user.assert_any_call(12, 'u2', 'Bob')
    linear_seadb_api.upsert_user.assert_any_call(12, 'u3', 'Carol')


def test_sync_linear_issue_skips_user_upsert_when_issue_is_up_to_date():
    handler = LinearIssueHandler()
    linear_api = Mock()
    linear_api.list_issues.return_value = ([
        {
            'id': 'issue-1',
            'updatedAt': '2026-01-02T00:00:00Z',
            'creator': {'id': 'u1', 'name': 'Alice'},
        }
    ], {'hasNextPage': False})

    linear_seadb_api = Mock()
    linear_seadb_api.get_issue_by_issue_id.return_value = {
        '_pk': 1,
        'issue_id': 'issue-1',
        'modified_time': '2026-01-03T00:00:00+00:00',
    }
    linear_seadb_api.get_linear_issues_count.return_value = 1

    handler._sync_linear_issue(
        12, None, {}, linear_api, Mock(), linear_seadb_api, 'team-1', True, False
    )

    linear_seadb_api.upsert_user.assert_not_called()
    linear_seadb_api.save_or_update_issue_to_db.assert_not_called()


def test_sync_linear_issue_publishes_completed_state_event():
    handler = LinearIssueHandler()
    handler.mq = Mock()

    existed_issue = {
        '_pk': 5,
        'issue_id': 'issue-1',
        'title': 'Fix login',
        'state': 'In Progress',
        'modified_time': '2026-01-01T00:00:00+00:00',
    }
    latest_issue = {
        **existed_issue,
        'state': 'Done',
        'modified_time': '2026-01-02T00:00:00+00:00',
        'closed_time': '2026-01-02T00:00:00+00:00',
    }

    linear_api = Mock()
    linear_api.list_issues.return_value = ([
        {
            'id': 'issue-1',
            'updatedAt': '2026-01-02T00:00:00Z',
            'title': 'Fix login',
            'state': {'name': 'Done', 'type': 'completed'},
        }
    ], {'hasNextPage': False})
    linear_api.list_issue_comments.return_value = ([], {'hasNextPage': False})
    linear_api.get_workflow_states.return_value = [
        {'id': 's1', 'name': 'In Progress', 'type': 'started'},
        {'id': 's2', 'name': 'Done', 'type': 'completed'},
    ]

    linear_seadb_api = Mock()
    linear_seadb_api.base_id = 'project-uuid'
    linear_seadb_api.get_issue_by_issue_id.side_effect = [existed_issue, latest_issue]
    linear_seadb_api.get_linear_issues_count.return_value = 1

    handler._sync_linear_issue(
        12, None, {}, linear_api, Mock(), linear_seadb_api, 'team-1', True, False
    )

    linear_api.get_workflow_states.assert_called_once_with(team_id='team-1')
    handler.mq.publish.assert_called_once()
    payload = json.loads(handler.mq.publish.call_args[0][1])
    assert payload['source_type'] == ConnectionType.LINEAR.value
    assert payload['event']['type'] == 'linear_issue_updated'
    assert payload['event']['old_value']['state'] == 'In Progress'
    assert payload['event']['old_value']['state_type'] == 'started'
    assert payload['event']['new_value']['state'] == 'Done'
    assert payload['event']['new_value']['state_type'] == 'completed'
