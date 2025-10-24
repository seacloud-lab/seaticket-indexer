import os
import sys
import logging
import subprocess
import hashlib
import json
import uuid
from datetime import datetime, timezone
from copy import deepcopy
from inspect import currentframe
from seaqa_indexer.utils.hasher import AESPasswordHasher


logger = logging.getLogger(__name__)
pyexec = None

ENCRYPT_KEYS = ['api_token', 'access_token', 'webhook_secret', 'api_key']


def find_in_path(prog):
    if 'win32' in sys.platform:
        sep = ';'
    else:
        sep = ':'

    dirs = os.environ['PATH'].split(sep)
    for d in dirs:
        d = d.strip()
        if d == '':
            continue
        path = os.path.join(d, prog)
        if os.path.exists(path):
            return path

    return None


def _get_python_executable():
    if sys.executable and os.path.isabs(sys.executable) and os.path.exists(sys.executable):
        return sys.executable

    try_list = [
        'python3.7',
        'python37',
        'python3',
    ]

    for prog in try_list:
        path = find_in_path(prog)
        if path is not None:
            return path

    path = os.environ.get('PYTHON', 'python')

    return path


def get_python_executable():
    # Find a suitable python executable
    global pyexec
    if pyexec is not None:
        return pyexec

    pyexec = _get_python_executable()
    return pyexec


def run(argv, cwd=None, env=None, suppress_stdout=False, suppress_stderr=False, output=None):
    def quote(args):
        return ' '.join(['"%s"' % arg for arg in args])

    cmdline = quote(argv)
    if cwd:
        logger.debug('Running command: %s, cwd = %s', cmdline, cwd)
    else:
        logger.debug('Running command: %s', cmdline)

    with open(os.devnull, 'w') as devnull:
        kwargs = dict(cwd=cwd, env=env, shell=True)

        if suppress_stdout:
            kwargs['stdout'] = devnull
        if suppress_stderr:
            kwargs['stderr'] = devnull

        if output:
            kwargs['stdout'] = output
            kwargs['stderr'] = output

        return subprocess.Popen(cmdline, **kwargs)


def md5(text):
    return hashlib.md5(text.encode()).hexdigest()


def time_difference_to_midnight():
    now = datetime.now()
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    time_diff = now - midnight
    seconds_diff = time_diff.total_seconds()
    seconds_diff = (24 * 60 * 60) - seconds_diff
    return seconds_diff


def time_str_to_utc_time(time_str):
    """Think of the time string as UTC time and convert it to datetime"""
    if not time_str.endswith('Z') and '+' not in time_str and '-' not in time_str[-6:]:
        time_str += '+00:00'

    if time_str.endswith('Z'):
        # python 3.12 can convert but 3.10 not support
        time_str = time_str[:-1] + '+00:00'
    dt = datetime.fromisoformat(time_str)
    return dt.astimezone(timezone.utc)


def decrypt_config(config):
    config_clone = deepcopy(config)
    cryptor = AESPasswordHasher()
    decrypted_details = {
        key: cryptor.decode(config_clone[key])
        for key in ENCRYPT_KEYS if key in config_clone and config_clone[key]
    }
    config_clone.update(decrypted_details)
    return config_clone


def encrypt_config(config):
    detail_clone = deepcopy(config)
    cryptor = AESPasswordHasher()
    encrypted_details = {
        key: cryptor.encode(detail_clone[key])
        for key in ENCRYPT_KEYS if key in detail_clone and detail_clone[key]
    }
    detail_clone.update(encrypted_details)
    return json.dumps(detail_clone)


def need_update_connection_status(item_count):
    from seaqa_indexer.utils.constants import CONNECTION_STATUS_UPDATE_LIMIT
    return item_count / CONNECTION_STATUS_UPDATE_LIMIT == item_count // CONNECTION_STATUS_UPDATE_LIMIT

def uuid_str_to_36_chars(uuid_str):
    if isinstance(uuid_str, uuid.UUID):
        return str(uuid_str)
    if len(uuid_str) == 32:
        return str(uuid.UUID(uuid_str))
    else:
        return uuid_str

def uuid_str_to_32_chars(project_uuid):
    if len(project_uuid) == 36:
        return uuid.UUID(project_uuid).hex
    else:
        return project_uuid


def need_update_connection_last_sync_count(item_count, last_update_time, last_update_count):
    from seaqa_indexer.utils.constants import CONNECTION_STATUS_MIN_UPDATE_COUNT, CONNECTION_STATUS_UPDATE_INTERVAL_SECONDS

    now = datetime.now()
    if last_update_time is None:
        last_update_time = now

    delta_count = item_count - last_update_count
    count_ready = delta_count >= CONNECTION_STATUS_MIN_UPDATE_COUNT

    elapsed_seconds = (now - last_update_time).total_seconds() if isinstance(last_update_time, datetime) else 0
    time_ready = elapsed_seconds >= CONNECTION_STATUS_UPDATE_INTERVAL_SECONDS

    should = count_ready or time_ready
    if should:
        return True, now, item_count
    else:
        return False, last_update_time, last_update_count


def get_log_line():
    frame = currentframe().f_back
    return frame.f_lineno
