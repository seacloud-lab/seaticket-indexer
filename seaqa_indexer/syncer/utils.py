import re


def upload_email_attachment(connection_id, project_uuid, storage, email):
    file_list = email.pop('attachment', [])
    filename2content_id = email.pop('filename2content_id', {})
    html_content = email.pop('html_content', '')
    message_id = email.get('message_id', '')
    filename2url = {}
    file_info_list = []
    for file in file_list:
        file_name = file.get('file_name')
        file_data = file.get('file_data')
        upload_file_name = message_id + '_' + file_name
        if file_data:
            storage.upload_file(connection_id, project_uuid, upload_file_name, file_data)
        file_info_list.append(file_name)
        filename2url[file_name] = file_name
    email['attachment'] = file_info_list

    # deal html content image
    # replace cid with real image url
    for file_name in filename2content_id:
        repl = filename2url.get(file_name)
        # repl maybe None if upload attachment fail
        if not repl:
            continue
        rep = re.compile(r'cid:%s' % re.escape(filename2content_id[file_name]))
        html_content = rep.sub(repl, html_content, 0)
    email['html_content'] = html_content
