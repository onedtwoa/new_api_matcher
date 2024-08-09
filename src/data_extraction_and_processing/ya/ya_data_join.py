import os
import pandas as pd
from src.settings import setup_logging
from src.config import get_current_datetime, BASE_DIR
from src.data_helper import LatestFileFetcher

script_name = os.path.splitext(os.path.basename(__file__))[0]
logger = setup_logging(script_name)
file_fetcher = LatestFileFetcher(logger)
OUTPUT_DIR = 'data/processing/yango_cars'
INPUT_DIR = 'data/raw/yango_cars'


def merge_csv_files(company_name):
    cars_dir = os.path.join(BASE_DIR, INPUT_DIR, company_name)
    models_dir = os.path.join(BASE_DIR, INPUT_DIR, company_name)

    cars_data = file_fetcher.get_and_load_latest_csv(cars_dir, '*yango_cars*.csv')
    models_data = file_fetcher.get_and_load_latest_csv(models_dir, '*yango_model*.csv')

    if cars_data.empty or models_data.empty:
        logger.error(f"<{company_name}> One or both of the CSV files are empty.")
        return

    models_data.rename(columns={
        'code': 'model_id',
        'manufacturer': 'merge_manufacturer',
        'short_name': 'merge_short_name',
        'name': 'merge_name'
    }, inplace=True)

    merged_data = pd.merge(cars_data, models_data, on='model_id', how='left')
    successful_merge = merged_data[~merged_data['merge_manufacturer'].isna()]
    unsuccessful_merge = merged_data[merged_data['merge_manufacturer'].isna()]

    output_dir = os.path.join(BASE_DIR, OUTPUT_DIR, company_name)
    os.makedirs(output_dir, exist_ok=True)

    success_output_file = os.path.join(output_dir, f'merged_yango_data_{get_current_datetime()}.csv')
    unsuccessful_output_file = os.path.join(output_dir, f'model_missing_yango_data_{get_current_datetime()}.csv')

    successful_merge.to_csv(success_output_file, index=False)
    logger.debug(f"<{company_name}> Successful merges saved to: {success_output_file}")

    unsuccessful_merge.to_csv(unsuccessful_output_file, index=False)
    logger.debug(f"<{company_name}> Unsuccessful merges saved to: {unsuccessful_output_file}")


if __name__ == "__main__":
    company_name = "ROTANA STAR RENT A CAR"
    merge_csv_files(company_name)
