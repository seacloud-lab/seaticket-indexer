import re
import datetime
import logging
import json

from seaqa_indexer.utils import time_to_utc_time, uuid_str_to_36_chars

logger = logging.getLogger(__name__)

EMAIL_SYNC_INTERVAL_DAYS = 10


def upload_email_attachment(connection_id, project_uuid, storage, email, pk):
    file_list = email.pop('attachments', [])
    filename2content_id = email.pop('filename2content_id', {})
    html_content = email.pop('html_content', '')
    attachment_list = []
    for file in file_list:
        file_name = file.get('file_name')
        file_data = file.get('file_data')
        content_disposition = file.get('content_disposition')
        upload_file_name = file_name
        if file_data:
            storage.upload_connection_record_file(project_uuid, connection_id, pk, upload_file_name, file_data)

        if content_disposition == 'attachment':
            attachment_list.append(upload_file_name)
    email['attachments'] = attachment_list

    # deal html content image
    # replace cid with real image url
    for file_name in filename2content_id:
        rep = re.compile(r'cid:%s' % re.escape(filename2content_id[file_name]))
        html_content = rep.sub(file_name, html_content, 0)
    email['html_content'] = html_content


def insert_and_update_thread_rows(email_seadb_api, connection_id, since, email_list, is_first_sync=False):
    email_list = sorted(email_list, key=lambda x: time_to_utc_time(x['email_date']))
    email_rows = []
    page_size = 100
    offset = 0
    while True:
        last_month_day = since - datetime.timedelta(days=30)
        last_month_day = last_month_day.isoformat()
        emails = email_seadb_api.get_threaded_emails_by_page(connection_id, last_month_day, page_size, offset)
        if not emails:
            break
        email_rows.extend(emails)
        offset += page_size

    message2thread = {email['message_id']: email['thread_id'] for email in email_rows}
    email_list = [email for email in email_list if not message2thread.get(email['message_id'])]

    # no_thread_reply_message_ids is the list of new emails' reply-ids who are not in last 30 days
    no_thread_reply_message_ids = []
    for email in email_list:
        if email['reply_to_message_id'] and not message2thread.get(email['reply_to_message_id']):
            no_thread_reply_message_ids.append(email['reply_to_message_id'])

    if no_thread_reply_message_ids:
        step = 100
        for i in range(0, len(no_thread_reply_message_ids), step):
            message_ids = no_thread_reply_message_ids[i: i+step]
            earlier_email_rows = email_seadb_api.get_thread_emails_by_message_ids(connection_id, message_ids)

            for email in earlier_email_rows:
                message2thread[email['message_id']] = email['thread_id']

    to_be_updated_thread_dict = {}
    # update email thread id
    for email in email_list:
        reply_to_id = email['reply_to_message_id']
        message_id = email['message_id']

        if reply_to_id in message2thread:  # checkout thread id from old message2thread
            thread_id = message2thread[reply_to_id]
            update_thread = {}

            message2thread[message_id] = thread_id
            if thread_id in to_be_updated_thread_dict:
                # update Last Updated
                if time_to_utc_time(email['email_date']) > time_to_utc_time(to_be_updated_thread_dict[thread_id]['last_updated_at']):
                    to_be_updated_thread_dict[thread_id]['last_updated_at'] = email['email_date']
                    update_thread['last_updated_at'] = email['email_date']
            else:
                to_be_updated_thread_dict[thread_id] = {
                    'last_updated_at': email['email_date']
                }
                update_thread['last_updated_at'] = email['email_date']
            if not email.get('is_sender'):
                update_thread['unread'] = not is_first_sync
            if update_thread:
                update_thread['thread_id'] = thread_id
                email_seadb_api.update_thread_to_db(connection_id, update_thread)
            email_seadb_api.update_email_thread_id(connection_id, message_id, thread_id)
        else:
            insert_thread = {
                'subject': email['subject'],
                'last_updated_at': email['email_date']
            }
            if not email.get('is_sender'):
                insert_thread['unread'] = not is_first_sync
            insert_thread_id = email_seadb_api.save_thread_to_db(connection_id, insert_thread)
            to_be_updated_thread_dict[insert_thread_id] = {
                'last_updated_at': email['email_date']
            }
            message2thread[message_id] = insert_thread_id
            email_seadb_api.update_email_thread_id(connection_id, message_id, insert_thread_id)


def send_connection_sync_result_msg(mq, project_uuid, connection_id, status):
    try:
        msg_content = json.dumps(
            {
                'project_uuid': uuid_str_to_36_chars(project_uuid), 
                'connection_id': connection_id, 
                'status': status,
                'type': 'connection-sync'
            }
        )
        if mq.publish('connection_sync', msg_content) > 0:
            logger.debug('Publish connection_sync event: %s' % msg_content)
        else:
            logger.info('No one subscribed to connection_sync channel, event (%s) has not been send' % msg_content)
    except Exception as e:
        logger.error('send sync connection finish msg failed, error: %s', e)
