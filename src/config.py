import os
import json
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.dirname(os.path.abspath(__file__))


def get_current_datetime():
    return datetime.now().strftime('%Y%m%d%H%M%S')


def load_config(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)


_config_json = load_config(os.path.join(BASE_DIR, 'config.json'))


class GoogleSheetsConfig:
    NAME_CREDENTIALS_GOOGLE = '__ya-excel-holds-40d515ea5b1d.json'

    def __init__(self):
        self.credentials_file = os.path.join(BASE_DIR, self.NAME_CREDENTIALS_GOOGLE)

