import os
from datetime import datetime, timedelta
from src.settings import setup_logging
from src.config import get_current_datetime, BASE_DIR
from src.yango_client import YangoAPIClient
from src.data_helper import CSVDataSaver

BASE_URL = 'https://drive.yango.tech'

LEASING_API_URL = "api/leasing/car/list"
BOOKINGS_API_URL = "api/leasing/rental/timetable"
MODEL_LIST_API_URL = "api/leasing/models/list"

DATA_DIR_CARS = os.path.join(BASE_DIR, 'data/raw/yango_cars')
DATA_DIR_BOOKINGS = os.path.join(BASE_DIR, 'data/raw/yango_bookings')

os.makedirs(DATA_DIR_CARS, exist_ok=True)
os.makedirs(DATA_DIR_BOOKINGS, exist_ok=True)

script_name = os.path.splitext(os.path.basename(__file__))[0]
logger = setup_logging(script_name)


def get_cars_leasing(client, company_name):
    logger.info(f"start get_cars_leasing")
    cars_with_pagination = client.fetch_all_cars_with_pagination(LEASING_API_URL)
    if cars_with_pagination:
        logger.info(f"{company_name} Fetched {len(cars_with_pagination)} "
                    f"cars from API with pagination for company {company_name}.")
        for car in cars_with_pagination:
            logger.debug(car)

        output_file = os.path.join(DATA_DIR_CARS, company_name, f'yango_cars_data_{get_current_datetime()}.csv')
        CSVDataSaver(logger).save_dict_to_csv(cars_with_pagination, output_file)
    else:
        logger.error(f"{company_name} Failed get_cars_leasing to fetch "
                     f"cars data with pagination for company {company_name}.")


def get_bookings(client, company_name):
    logger.info(f"{company_name} start get_bookings")
    now = datetime.now()
    since_timestamp = int((now - timedelta(days=10)).timestamp())
    until_timestamp = int((now + timedelta(days=80)).timestamp())

    params = {
        'since': since_timestamp,
        'until': until_timestamp,
        'timeout': '27000000',
        'lang': 'en'
    }
    output_file = os.path.join(DATA_DIR_BOOKINGS, company_name, f'yango_bookings_data_{get_current_datetime()}.csv')
    all_bookings = client.fetch_bookings(BOOKINGS_API_URL, params)
    if all_bookings:
        logger.info(f"{company_name} Fetched {len(all_bookings)} total bookings from API for company {company_name}.")
        for booking in all_bookings:
            logger.debug(booking)
        CSVDataSaver(logger).save_dict_to_csv(all_bookings, output_file)
    else:
        logger.error(f"{company_name} Failed get_bookings to fetch bookings data for company {company_name}.")


def get_model_list(client, company_name):
    logger.info(f"start get_model_list")
    model_list = client.fetch_model_list(MODEL_LIST_API_URL)
    if model_list:
        logger.info(f"{company_name} Fetched {len(model_list)} models from API for company {company_name}.")
        for model in model_list:
            logger.debug(model)

        output_file = os.path.join(DATA_DIR_CARS, company_name, f'yango_model_list_{get_current_datetime()}.csv')
        CSVDataSaver(logger).save_dict_to_csv(model_list, output_file)
    else:
        logger.error(f"{company_name} Failed get_model_list to fetch model list for company {company_name}.")


def main(company_name, token_drive_ya_tech):
    client = YangoAPIClient(BASE_URL, token_drive_ya_tech, logger)
    get_cars_leasing(client, company_name)
    get_bookings(client, company_name)
    get_model_list(client, company_name)


if __name__ == "__main__":
    from src.config import _config_json
    company_name = "AL EMAD CAR RENTAL"
    token_drive_ya_tech = _config_json["ya_companies"][company_name]["TOKEN_DRIVE_YA_TECH"]
    main(company_name, token_drive_ya_tech)
