import os
from datetime import datetime, timedelta
from src.settings import setup_logging
from src.config import BASE_DIR

script_name = os.path.splitext(os.path.basename(__file__))[0]
logger = setup_logging(script_name)

DATA_PATH = os.path.join(BASE_DIR, 'data')
LOGS_PATH = os.path.join(BASE_DIR, 'logs')
FILE_EXTENSIONS = ['.csv', '.log', '.json']
TIME_THRESHOLD = timedelta(minutes=1)


def delete_old_files(directory, file_extensions, time_threshold):
    current_time = datetime.now()
    for root, dirs, files in os.walk(directory):
        for file in files:
            if any(file.endswith(ext) for ext in file_extensions):
                file_path = os.path.join(root, file)
                file_mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                if current_time - file_mod_time > time_threshold:
                    try:
                        os.remove(file_path)
                        logger.debug(f"Deleted file: {file_path}")
                    except Exception as e:
                        logger.error(f"Failed to delete {file_path}: {e}")


def main():
    delete_old_files(DATA_PATH, FILE_EXTENSIONS, TIME_THRESHOLD)
    delete_old_files(LOGS_PATH, FILE_EXTENSIONS, TIME_THRESHOLD)


if __name__ == "__main__":
    main()
