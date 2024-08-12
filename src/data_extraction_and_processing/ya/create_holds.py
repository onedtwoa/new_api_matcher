import os
import time
from src.settings import setup_logging
from src.yango_client import YangoAPIClient
from src.config import get_current_datetime, BASE_URL, BASE_DIR
from src.data_helper import LatestFileFetcher, CSVDataSaver

script_name = os.path.splitext(os.path.basename(__file__))[0]
logger = setup_logging(script_name)
file_fetcher = LatestFileFetcher(logger)

TAG_API_URL = 'api/leasing/car/tag/add'
MAX_RETRIES = 3
RETRY_DELAY = 2
MAX_RECURSION_DEPTH = 5
HOLDS_DIR = 'data/final'


def add_tag_to_car(client, api_url, car_id, hold_start, hold_end, tag_name, hold_comment):
    params = {
        "car_id": car_id,
        "since": hold_start,
        "until": hold_end,
        "hold_comment": hold_comment
    }
    string_params = {
        "tag_name": tag_name,
        "timeout": 27000000,
        "lang": "en"
    }

    for attempt in range(MAX_RETRIES):
        logger.info(f"Sending request with params: {params} and string_params: {string_params}")
        response = client.add_hold_car(api_url, params, string_params)

        if response is not None:
            if 'tagged_objects' in response and response['tagged_objects']:
                logger.info(f"Successfully added tag to car: {car_id}")
                return response
            else:
                logger.error(f"Failed to add tag to car: {response}")
                return None
        else:
            logger.warning(f"Internal server error for {hold_comment}\n"
                           f"Retrying {attempt + 1}/{MAX_RETRIES}...")
            time.sleep(RETRY_DELAY)
    logger.error(f"Failed to add tag to car after {MAX_RETRIES} attempts: {params}")
    return None


def create_data_for_hold(data):
    records = []
    for index, row in data.iterrows():
        car_id = row['ya_id']
        requested_since = row['current_since']
        requested_until = row['current_until']
        since_dubai = row['current_since_Dubai']
        until_dubai = row['current_until_Dubai']
        plate_no = row['ya_number']
        if row.get('takamol_CarName'):
            takamol_car_key = row['takamol_CarKey']
            takamol_car_name = row['takamol_CarName']
            message = f"takamol\n{takamol_car_name} ({takamol_car_key}) with number {plate_no}\n" \
                      f"hold: from {since_dubai} to {until_dubai} ({requested_since}, {requested_until})"
        elif row.get('sheet_Status'):
            google_car_key = row['sheet_PlateNo']
            sheet_vehicle_type = row['sheet_VehicleType']
            status = row['sheet_Status']
            message = f"google docs \n{sheet_vehicle_type} ({google_car_key}) with number {plate_no}\n" \
                      f"hold ({status}): from {since_dubai} to {until_dubai} ({requested_since}, {requested_until})"
        else:
            raise ValueError("Unknown source")

        record = {
            "car_id": car_id,
            "requested_since": truncate_timestamp(requested_since, 'microseconds'),
            "requested_until": truncate_timestamp(requested_until, 'microseconds'),
            "message": message
        }
        records.append(record)

    return records


def truncate_timestamp(timestamp, to):
    if to not in {'microseconds'}:
        raise ValueError("Unsupported truncation level")

    timestamp_str = str(timestamp).split('.')[0]
    if to == 'microseconds':
        return int(timestamp_str.ljust(16, '0'))


def main(company_name, token_drive_ya_tech, tag_name):
    client = YangoAPIClient(BASE_URL, token_drive_ya_tech, logger)
    full_dir_data_holds = os.path.join(BASE_DIR, HOLDS_DIR, company_name)
    records = []
    ready_to_load_latest_csv = file_fetcher.get_and_load_latest_csv(full_dir_data_holds, 'ready_to_load_*.csv')
    ready_to_load_data = create_data_for_hold(ready_to_load_latest_csv)

    total_records = len(ready_to_load_data)
    successful_holds = 0
    failed_holds = 0

    for record in ready_to_load_data:
        try:
            response = add_tag_to_car(client, TAG_API_URL,
                                      car_id=record['car_id'],
                                      hold_start=record['requested_since'],
                                      hold_end=record['requested_until'],
                                      hold_comment=record['message'],
                                      tag_name=tag_name)
            if response and response.get('tagged_objects'):
                successful_holds += 1
            else:
                failed_holds += 1
            if response is not None:
                records.append(response)
        except Exception as e:
            logger.fatal(f"<{company_name}> Error add add_tag_to_car for {record}: {e}")
            failed_holds += 1

    dir4records = os.path.join(full_dir_data_holds, f'successfully_records_{get_current_datetime()}.csv')
    if records:
        CSVDataSaver(logger).save_dict_to_csv(records, dir4records)

    logger.info(f"<{company_name}> Records saved to {dir4records}")
    logger.info(f"<{company_name}> Total records: {total_records}")
    logger.info(f"<{company_name}> Successfully placed holds: {successful_holds}")
    logger.info(f"<{company_name}> Failed to place holds: {failed_holds}")


if __name__ == "__main__":
    from src.config import _config_json

    company_name = "A M G A RENT A CAR L.L.C"
    token_drive_ya_tech = _config_json["ya_companies"][company_name]["TOKEN_DRIVE_YA_TECH"]
    tag_name = _config_json["ya_companies"][company_name]["tag_name"]
    main(company_name, token_drive_ya_tech, tag_name)
