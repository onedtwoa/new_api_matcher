import os
import pandas as pd
from datetime import datetime, timedelta
from src.settings import setup_logging
from src.config import get_current_datetime, BASE_DIR
from src.data_helper import LatestFileFetcher, CSVDataSaver, JSONDataSaver, TZ_DUBAI, find_non_overlapping_intervals

script_name = os.path.splitext(os.path.basename(__file__))[0]
logger = setup_logging(script_name)
file_fetcher = LatestFileFetcher(logger)
data_saver = CSVDataSaver(logger)
json_saver = JSONDataSaver(logger)

RES_DIR = os.path.join(BASE_DIR, 'data/final/')
os.makedirs(RES_DIR, exist_ok=True)


def get_intervals_for_default_booking():
    now = datetime.now(TZ_DUBAI)
    since = int(now.timestamp())
    until = int((now + timedelta(hours=24)).timestamp())
    since_dubai = now.strftime('%m/%d/%Y %I:%M:%S %p')
    until_dubai = (now + timedelta(hours=24)).strftime('%m/%d/%Y %I:%M:%S %p')
    return [(since, until, since_dubai, until_dubai)]


def merge_data(bookings_data, matched_data):
    logger.info("Starting merge_data")
    logger.debug(f"Initial matched_data columns: {matched_data.columns}")

    matched_data['sheet_Status'] = matched_data['sheet_Status'].str.lower().str.replace(' ', '')
    filtered_matched_data = matched_data[matched_data['sheet_Status'] != 'available']
    logger.debug(f"Filtered matched_data: {filtered_matched_data.head()}")

    results = []
    current_time = datetime.now(TZ_DUBAI).timestamp()

    for index, row in filtered_matched_data.iterrows():
        logger.debug(f"Processing row: {row}")
        new_intervals = get_intervals_for_default_booking()
        existing_intervals = bookings_data[bookings_data['id_car'] == row['ya_id']][['since', 'until']].values.tolist()
        logger.debug(f"Existing intervals: {existing_intervals}")
        non_overlapping_intervals = find_non_overlapping_intervals(new_intervals, existing_intervals, row['ya_id'],
                                                                   logger)
        logger.debug(f"Non-overlapping intervals: {non_overlapping_intervals}")
        for interval in non_overlapping_intervals:
            if interval[1] > current_time:
                new_row = row.copy()
                new_row['current_since'] = interval[0]
                new_row['current_until'] = interval[1]
                new_row['current_since_Dubai'] = interval[2]
                new_row['current_until_Dubai'] = interval[3]
                results.append(new_row)
                logger.debug(f"Appended new_row: {new_row}")

    merged_data = pd.DataFrame(results)
    logger.debug(f"Final merged_data: {merged_data.head()}")

    logger.info("Finished merge_data")
    return merged_data


def main(company_name):
    bookings_data, matched_data = file_fetcher.load_data_for_preparing_for_load_script(company_name)

    if matched_data.empty:
        return

    merged_data = merge_data(bookings_data, matched_data)
    data_saver.save_dataframe_to_csv(merged_data,
                                     os.path.join(RES_DIR, company_name, f"ready_to_load_{get_current_datetime()}.csv"))
    logger.info("prepare_for_loading finished successfully")


if __name__ == "__main__":
    company_name = "AL EMAD CAR RENTAL"
    main(company_name)
