import json
import os
import pandas as pd
from datetime import datetime
import pytz
from src.settings import setup_logging
from src.config import get_current_datetime, BASE_DIR
from src.data_helper import LatestFileFetcher, \
    CSVDataSaver, DataNormalizer, JSONDataSaver

script_name = os.path.splitext(os.path.basename(__file__))[0]
logger = setup_logging(script_name)
file_fetcher = LatestFileFetcher(logger)
data_saver = CSVDataSaver(logger)
json_saver = JSONDataSaver(logger)
TZ_DUBAI = pytz.timezone('Asia/Dubai')

YA_BOOKINGS_DIR = 'data/raw/yango_bookings'
MATCHED_DATA_DIR = 'data/processing/yango_cars'
RES_DIR = os.path.join(BASE_DIR, 'data/final/')
os.makedirs(RES_DIR, exist_ok=True)


def load_data(company_name):
    bookings_data = file_fetcher.get_and_load_latest_csv(os.path.join(BASE_DIR, YA_BOOKINGS_DIR,
                                                                      company_name), '*yango_bookings*.csv')
    matched_data = file_fetcher.get_and_load_latest_csv(os.path.join(BASE_DIR, MATCHED_DATA_DIR,
                                                                     company_name), '*_matched_*.csv')
    if bookings_data is None or bookings_data.empty:
        logger.warning(f"{company_name} Данные бронирований не найдены или пусты.")
        bookings_data = pd.DataFrame()

    if matched_data is None or matched_data.empty:
        logger.warning(f"{company_name} Данные по мэтчам не найдены или пусты.")
        matched_data = pd.DataFrame()

    return bookings_data, matched_data


def normalize_dates(reservation):
    try:
        res_start = datetime.strptime(reservation['FromDateTime'], '%m/%d/%Y %I:%M:%S %p')
        res_end = datetime.strptime(reservation['ToDateTime'], '%m/%d/%Y %I:%M:%S %p')
        res_start = TZ_DUBAI.localize(res_start)
        res_end = TZ_DUBAI.localize(res_end)
        reservation['since'] = res_start.timestamp()
        reservation['until'] = res_end.timestamp()
        reservation['since_Dubai'] = res_start.strftime('%m/%d/%Y %I:%M:%S %p')
        reservation['until_Dubai'] = res_end.strftime('%m/%d/%Y %I:%M:%S %p')
    except ValueError as e:
        logger.error(f"Ошибка преобразования даты для reservation {reservation}: {e}")
        raise


def safe_json_loads(x):
    try:
        if pd.isna(x):
            return []
        x = DataNormalizer.convert_to_json_format(x)
        return json.loads(x)
    except json.JSONDecodeError as e:
        logger.error(f"Ошибка декодирования JSON: {x} — {e}")
        return []


def get_intervals(reservations):
    intervals = []
    for res in reservations:
        normalize_dates(res)
        intervals.append((res['since'], res['until'], res['since_Dubai'], res['until_Dubai']))
    return intervals


def find_non_overlapping_intervals(new_intervals, existing_intervals, ya_id):
    result_intervals = []
    for new_start, new_end, new_start_dubai, new_end_dubai in new_intervals:
        overlaps = False
        for exist_start, exist_end in existing_intervals:
            if new_start < exist_end and new_end > exist_start:
                overlaps = True
                logger.debug(f"Found overlapping interval for ya_id={ya_id}: "
                            f"New Interval [{new_start}, {new_end}] "
                            f"with Existing Interval [{exist_start}, {exist_end}]")
                if new_start < exist_start:
                    result_intervals.append((new_start, exist_start, new_start_dubai,
                           datetime.fromtimestamp(exist_start, TZ_DUBAI).strftime('%m/%d/%Y %I:%M:%S %p')))
                if new_end > exist_end:
                    result_intervals.append((exist_end, new_end,
                           datetime.fromtimestamp(exist_end, TZ_DUBAI).strftime('%m/%d/%Y %I:%M:%S %p'), new_end_dubai))
        if not overlaps:
            result_intervals.append((new_start, new_end, new_start_dubai, new_end_dubai))
    return result_intervals


def merge_data(bookings_data, matched_data):
    logger.info("Starting merge_data")
    logger.debug(f"Initial matched_data columns: {matched_data.columns}")

    matched_data['takamol_Reservations'] = matched_data['takamol_Reservations'].apply(
        lambda x: safe_json_loads(x) if x else [])
    logger.debug(f"After applying safe_json_loads: {matched_data.head()}")

    filtered_matched_data = matched_data[matched_data['takamol_Reservations'].str.len() > 0]
    logger.debug(f"Filtered matched_data: {filtered_matched_data.head()}")

    results = []
    current_time = datetime.now(TZ_DUBAI).timestamp()

    for index, row in filtered_matched_data.iterrows():
        logger.debug(f"Processing row: {row}")
        new_intervals = get_intervals(row['takamol_Reservations'])
        existing_intervals = bookings_data[bookings_data['id_car'] == row['ya_id']][['since', 'until']].values.tolist()
        non_overlapping_intervals = find_non_overlapping_intervals(new_intervals, existing_intervals, row['ya_id'])

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

    logger.info("Finished merge_data")
    return merged_data


def main(company_name):
    bookings_data, matched_data = load_data(company_name)
    merged_data = merge_data(bookings_data, matched_data)
    data_saver.save_dataframe_to_csv(merged_data, os.path.join(RES_DIR, company_name,
                                                               f"ready_to_load_{get_current_datetime()}.csv"))
    logger.info("prepare_for_loading finished successfully")


if __name__ == "__main__":
    company_name = "AUTOBOTS RENT A CAR L.L.C"
    main(company_name)
