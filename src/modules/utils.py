import json
import os.path as path
from definitions import DATA_DIR
from datetime import datetime


def unix_time_millis():
    return int(datetime.now().timestamp() * 1000)


def get_current_time():
    return datetime.now().astimezone().replace(microsecond=0).isoformat()


def write_json_file(json_data, file_name):
    file_path = path.join(DATA_DIR, file_name)
    with open(file_path, "wt") as f:
        json.dump(json_data, f, sort_keys=True, indent=2, ensure_ascii=True)
