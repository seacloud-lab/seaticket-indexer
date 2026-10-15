from unittest.mock import Mock, patch

import pytest

from seaqa_indexer.utils.mail_viewer.gmail_oauth_mail_viewer import GmailOAuthMailViewer
from seaqa_indexer.utils.mail_viewer.microsoft_oauth_mail_viewer import MicrosoftOAuthMailViewer
from seaqa_indexer.utils.mail_viewer.exceptions import EmailConfigError


def make_viewer(provider='Gmail', account_type='alias'):
    viewer_class = GmailOAuthMailViewer if provider == 'Gmail' else MicrosoftOAuthMailViewer
    return viewer_class(
        {'server_provider': provider, 'account_type': account_type, 'sender_email': 'alias@example.com'},
        {'refresh_token': 'refresh', 'access_token': 'access', 'expires_at': 9999999999},
        {'client_id': 'id', 'client_secret': 'secret', 'token_url': 'https://example.com/token'},
    )


@pytest.mark.parametrize('provider,account_type', [('Gmail', 'alias'), ('Microsoft', 'alias'), ('Microsoft', 'shared')])
@pytest.mark.parametrize('header', ['From', 'To', 'Cc', 'Bcc', 'Delivered-To', 'X-Original-To'])
def test_target_address_matches_all_supported_headers(provider, account_type, header):
    viewer = make_viewer(provider, account_type)
    msg = viewer._parse_message(f'{header}: Support <ALIAS@example.com>\n\nbody')
    assert viewer.is_associated_message(msg, {})


@pytest.mark.parametrize('headers,body', [
    ('From: other@example.com\nTo: unrelated@example.com', 'alias@example.com'),
    ('From: otheralias@example.com', ''),
    ('To: alias+tag@example.com', ''),
    ('From: "alias@example.com" <other@example.com>', ''),
    ('Reply-To: alias@example.com\nReturn-Path: <alias@example.com>', ''),
    ('Subject: alias@example.com', ''),
])
def test_unrelated_addresses_and_body_do_not_match(headers, body):
    viewer = make_viewer()
    assert not viewer.is_associated_message(viewer._parse_message(headers + '\n\n' + body), {})


def test_repeated_delivery_headers_and_provider_bcc_metadata():
    viewer = make_viewer('Microsoft')
    msg = viewer._parse_message('Delivered-To: other@example.com\nDelivered-To: alias@example.com\n\n')
    assert viewer.is_associated_message(msg, {})
    msg = viewer._parse_message('From: other@example.com\n\n')
    assert viewer.is_associated_message(msg, {'participant_addresses': ['alias@example.com']})


def test_attached_forward_headers_do_not_match():
    viewer = make_viewer()
    msg = viewer._parse_message(
        'From: other@example.com\nTo: other@example.com\n'
        'Content-Type: multipart/mixed; boundary="b"\n\n--b\n'
        'Content-Type: message/rfc822\n\nFrom: alias@example.com\n\nforwarded\n--b--\n'
    )
    assert not viewer.is_associated_message(msg, {})


def test_filters_before_content_and_attachment_processing_and_continues():
    viewer = make_viewer()
    refs = [{'id': 'unrelated'}, {'id': 'matching'}]
    raw_messages = [b'From: other@example.com\nTo: other@example.com\n\nbody',
                    b'From: alias@example.com\nTo: other@example.com\nMessage-ID: <matching>\nDate: Wed, 14 Oct 2026 12:00:00 +0000\n\nbody']
    with patch.object(viewer, 'iter_message_refs', return_value=iter(refs)), \
            patch.object(viewer, 'fetch_raw_message', side_effect=raw_messages), \
            patch.object(viewer, 'get_content', return_value=('body', '')) as content, \
            patch.object(viewer, 'get_attachments', return_value=([], {})) as attachments:
        results = list(viewer.search_emails('2026-01-01', '2026-01-02'))
    assert len(results) == 1
    assert results[0]['message_id'] == '<matching>'
    content.assert_called_once()
    attachments.assert_called_once()


def test_personal_is_unfiltered_and_mailbox_principals():
    viewer = make_viewer('Gmail', 'personal')
    assert viewer.is_associated_message(viewer._parse_message('From: other@example.com\n\n'), {})
    assert make_viewer().list_url.endswith('/users/me/messages')
    assert make_viewer('Microsoft').base_url.endswith('/me')
    assert make_viewer('Microsoft', 'shared').base_url.endswith('/users/alias%40example.com')
    with pytest.raises(EmailConfigError):
        make_viewer('Gmail', 'shared')


def test_microsoft_pagination_keeps_recipient_metadata():
    viewer = make_viewer('Microsoft', 'shared')
    viewer._request_json = Mock(side_effect=[
        {'value': [], '@odata.nextLink': 'https://next-page'},
        {'value': [{'id': 'match', 'bccRecipients': [{'emailAddress': {'address': 'alias@example.com'}}]}]},
        {'value': []},
    ])
    refs = list(viewer.iter_message_refs('2026-01-01', '2026-01-02'))
    assert refs[0]['participant_addresses'] == ['', 'alias@example.com']
    assert viewer._request_json.call_args_list[1].kwargs['params'] is None
