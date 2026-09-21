import json
from unittest.mock import Mock

from seaqa_indexer.handlers.jira_handler import JiraIssueHandler
from seaqa_indexer.utils.constants import ConnectionType


def test_sync_jira_issues_publishes_completed_status_event():
    handler = JiraIssueHandler()
    handler.mq = Mock()

    existed_issue = {
        '_pk': 8,
        'issue_id': 101,
        'issue_key': 'PROJ-1',
        'title': 'Fix login',
        'status': 'In Progress',
        'modified_time': '2026-01-01T00:00:00+00:00',
    }
    latest_issue = {
        **existed_issue,
        'status': 'Done',
        'modified_time': '2026-01-02T00:00:00+00:00',
    }

    jira_api = Mock()
    jira_api.search_issues.return_value = {
        'issues': [{
            'id': '101',
            'key': 'PROJ-1',
            'fields': {
                'summary': 'Fix login',
                'updated': '2026-01-02T00:00:00+00:00',
                'status': {
                    'name': 'Done',
                    'statusCategory': {'key': 'done'},
                },
            },
        }],
        'total': 1,
    }
    jira_api.get_issue_comments.return_value = []
    jira_api.get_statuses.return_value = [
        {'id': '1', 'name': 'In Progress', 'statusCategory': {'key': 'indeterminate'}},
        {'id': '2', 'name': 'Done', 'statusCategory': {'key': 'done'}},
    ]

    jira_seadb_api = Mock()
    jira_seadb_api.get_issue_by_issue_id.side_effect = [existed_issue, latest_issue]
    jira_seadb_api.get_jira_issues_count.return_value = 1

    handler._sync_jira_issues(
        12,
        'project-uuid',
        jira_api,
        'site-1',
        'PROJ',
        '2026-01-01T00:00:00+00:00',
        {},
        Mock(),
        jira_seadb_api,
        False,
    )

    jira_api.get_statuses.assert_called_once_with('PROJ', 'site-1')
    handler.mq.publish.assert_called_once()
    payload = json.loads(handler.mq.publish.call_args[0][1])
    assert payload['source_type'] == ConnectionType.JIRA_ISSUE.value
    assert payload['event']['type'] == 'jira_issue_updated'
    assert payload['event']['old_value']['status'] == 'In Progress'
    assert payload['event']['old_value']['status_category'] == 'indeterminate'
    assert payload['event']['new_value']['status'] == 'Done'
    assert payload['event']['new_value']['status_category'] == 'done'
