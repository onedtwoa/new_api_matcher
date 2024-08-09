import os
import pandas as pd
from datetime import datetime
from src.settings import setup_logging
from src.config import get_current_datetime, BASE_DIR
from src.data_helper import LatestFileFetcher, CSVDataSaver, JSONDataSaver,merge_overlapping_intervals, \
                                TZ_DUBAI, find_non_overlapping_intervals, safe_json_loads

script_name = os.path.splitext(os.path.basename(__file__))[0]
logger = setup_logging(script_name)
file_fetcher = LatestFileFetcher(logger)
data_saver = CSVDataSaver(logger)
json_saver = JSONDataSaver(logger)

RES_DIR = os.path.join(BASE_DIR, 'data/final/')
os.makedirs(RES_DIR, exist_ok=True)


def normalize_dates(reservation):
    try:
        logger.debug(f"Original FromDateTime: {reservation['FromDateTime']}, ToDateTime: {reservation['ToDateTime']}")

        res_start = datetime.strptime(reservation['FromDateTime'], '%m/%d/%Y %I:%M:%S %p')
        res_end = datetime.strptime(reservation['ToDateTime'], '%m/%d/%Y %I:%M:%S %p')
        logger.debug(f"Parsed FromDateTime: {res_start}, ToDateTime: {res_end}")

        res_start = res_start
        res_end = res_end
        logger.debug(f"Localized FromDateTime (Dubai): {res_start}, ToDateTime (Dubai): {res_end}")

        reservation['since'] = int(res_start.timestamp())
        reservation['until'] = int(res_end.timestamp())
        logger.debug(f"Unix Timestamps: since = {reservation['since']}, until = {reservation['until']}")

        reservation['since_Dubai'] = res_start.strftime('%m/%d/%Y %I:%M:%S %p')
        reservation['until_Dubai'] = res_end.strftime('%m/%d/%Y %I:%M:%S %p')
        logger.debug(
            f"Formatted Dates (Dubai): since_Dubai = {reservation['since_Dubai']},"
            f" until_Dubai = {reservation['until_Dubai']}")

    except ValueError as e:
        logger.error(f"Ошибка преобразования даты для reservation {reservation}: {e}")
        raise


def get_intervals(reservations):
    intervals = []
    for res in reservations:
        normalize_dates(res)
        intervals.append((res['since'], res['until'], res['since_Dubai'], res['until_Dubai']))
    return intervals


def merge_data(bookings_data, matched_data):
    logger.debug("Starting merge_data")
    logger.debug(f"Initial matched_data columns: {matched_data.columns}")

    matched_data['takamol_Reservations'] = matched_data['takamol_Reservations'].apply(
        lambda x: safe_json_loads(x, logger) if x else [])
    logger.debug(f"After applying safe_json_loads: {matched_data.head()}")

    filtered_matched_data = matched_data[matched_data['takamol_Reservations'].str.len() > 0]
    logger.debug(f"Filtered matched_data: {filtered_matched_data.head()}")

    results = []
    current_time = datetime.now(TZ_DUBAI).timestamp()

    for index, row in filtered_matched_data.iterrows():
        logger.debug(f"Processing row: {row}")
        new_intervals = get_intervals(row['takamol_Reservations'])
        existing_intervals = bookings_data[bookings_data['id_car'] == row['ya_id']][['since', 'until']].values.tolist()
        non_overlapping_new_intervals = merge_overlapping_intervals(new_intervals)
        non_overlapping_intervals = find_non_overlapping_intervals(non_overlapping_new_intervals, existing_intervals,
                                                                   row['ya_id'], logger)
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
    logger.debug(f"Final merged_data before dropping column: {merged_data.head()}")

    if not merged_data.empty:
        merged_data.drop(columns=['takamol_Reservations'], inplace=True)
    logger.debug(f"Final merged_data after dropping column: {merged_data.head()}")

    logger.debug("Finished merge_data")
    return merged_data


def main(company_name):
    bookings_data, matched_data = file_fetcher.load_data_for_preparing_for_load_script(company_name)

    if matched_data.empty:
        return

    merged_data = merge_data(bookings_data, matched_data)
    data_saver.save_dataframe_to_csv(merged_data, os.path.join(RES_DIR, company_name,
                                                               f"ready_to_load_{get_current_datetime()}.csv"))
    logger.info(f"<{company_name}> prepare_for_loading finished successfully")


if __name__ == "__main__":
    company_name = "ROTANA STAR RENT A CAR"
    main(company_name)
