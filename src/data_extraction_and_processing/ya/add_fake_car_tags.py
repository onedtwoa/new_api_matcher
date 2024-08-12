import os
import time
from src.settings import setup_logging
from src.yango_client import YangoAPIClient
from src.config import BASE_URL, BASE_DIR
from src.data_helper import LatestFileFetcher

script_name = os.path.splitext(os.path.basename(__file__))[0]
logger = setup_logging(script_name)
file_fetcher = LatestFileFetcher(logger)

TAG_API_URL = 'api/leasing/car/tag/add'
FAKE_CARS_DIR = 'data/raw/yango_cars'


def add_fake_tag_to_car(client, api_url, car_id):
    logger.info(f"Sending request to add fake_car tag to car_id: {car_id}")
    response = client.add_fake_car(api_url, car_id)

    if response is not None:
        if 'tagged_objects' in response and response['tagged_objects']:
            logger.info(f"Successfully added fake_car tag to car: {car_id}")
            return response
        else:
            logger.error(f"Failed to add fake_car tag to car: {car_id}. Response: {response}")
            return None
    return None


def main(company_name, token_drive_ya_tech):
    client = YangoAPIClient(BASE_URL, token_drive_ya_tech, logger)
    full_dir_data_fake_cars = os.path.join(BASE_DIR, FAKE_CARS_DIR, company_name)
    fake_cars_data = file_fetcher.get_and_load_latest_csv(full_dir_data_fake_cars, 'yango_duplicates_*.csv')

    total_records = len(fake_cars_data)
    successful_tags = 0
    failed_tags = 0

    for _, row in fake_cars_data.iterrows():
        car_id = row['id']
        try:
            add_fake_tag_to_car(client, TAG_API_URL, car_id)
            successful_tags += 1
        except Exception as e:
            logger.fatal(f"<{company_name}> Error adding fake_car tag to car_id={car_id}: {e}")
            failed_tags += 1

    logger.info(f"<{company_name}> Total records: {total_records}")
    logger.info(f"<{company_name}> Successfully added tags: {successful_tags}")
    logger.info(f"<{company_name}> Failed to add tags: {failed_tags}")


if __name__ == "__main__":
    from src.config import _config_json

    company_name = "HEXA CAR RENTAL"
    token_drive_ya_tech = _config_json["ya_companies"][company_name]["TOKEN_DRIVE_YA_TECH"]
    main(company_name, token_drive_ya_tech)
