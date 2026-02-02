import datetime
import os
from datetime import timezone
from seaqa_indexer.utils.constants import CONNECTION_STATUS_MIN_UPDATE_COUNT, CONNECTION_STATUS_UPDATE_INTERVAL_SECONDS


def time_to_utc_time(time_data):
    """Think of the time as UTC time and convert it to datetime"""
    if isinstance(time_data, int):
        return datetime.fromtimestamp(time_data, tz=timezone.utc)

    if not time_data.endswith('Z') and '+' not in time_data and '-' not in time_data[-6:]:
        time_data += '+00:00'

    if time_data.endswith('Z'):
        # python 3.12 can convert but 3.10 not support
        time_data = time_data[:-1] + '+00:00'
    dt = datetime.fromisoformat(time_data)
    return dt.astimezone(timezone.utc)

def need_update_connection_last_sync_count(item_count, last_update_time, last_update_count):
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

def get_file_suffix(filename):
    return os.path.splitext(filename)[1].strip(".").lower()
