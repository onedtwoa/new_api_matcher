import os
import csv
import pandas as pd
import glob
import json
import re
from datetime import datetime
from src.config import BASE_DIR
import pytz

TZ_DUBAI = pytz.timezone('Asia/Dubai')


class CSVDataSaver:
    def __init__(self, logger):
        self.logger = logger

    @staticmethod
    def get_headers(data):
        headers = set()
        for item in data:
            headers.update(item.keys())
        return list(headers)

    def save_dict_to_csv(self, data, filename):
        headers = self.get_headers(data)
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        try:
            with open(filename, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=headers)
                writer.writeheader()
                if not data:
                    self.logger.debug("No data to save. Creating an empty CSV file.")
                else:
                    for item in data:
                        writer.writerow(item)
                    self.logger.debug(f"Data saved to {filename}")
        except IOError as e:
            self.logger.error(f"Failed to save data: {e}")

    def save_dataframe_to_csv(self, df, filename):
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        try:
            df.to_csv(filename, index=False)
            self.logger.debug(f"Data saved to {filename}")
        except Exception as e:
            self.logger.error(f"Error saving data to CSV: {e}")


class LatestFileFetcher:
    def __init__(self, logger):
        self.logger = logger

    def get_latest_file(self, directory, pattern):
        try:
            files = glob.glob(os.path.join(directory, pattern))
            if not files:
                raise FileNotFoundError(f"No files matching the pattern {pattern} found in directory {directory}")
            latest_file = max(files, key=os.path.getctime)
            self.logger.debug(f"Latest file found: {latest_file}")
            return latest_file
        except Exception as e:
            self.logger.warning(f"Error getting the latest file: {e}")
            raise

    def get_and_load_latest_json(self, directory, pattern):
        self.logger.debug(f"Fetching latest JSON file from {directory} with pattern {pattern}")
        try:
            json_path = self.get_latest_file(directory, pattern)
            with open(json_path, mode='r', encoding='utf-8') as json_file:
                data = json.load(json_file)
            self.logger.debug(f"Loaded file: {json_path}")
            return data
        except FileNotFoundError as e:
            self.logger.warning(f"No JSON files found: {e}")
            return {}
        except Exception as e:
            self.logger.error(f"Error loading JSON file from {directory}: {e}")
            return {}

    def get_and_load_latest_csv(self, directory, pattern):
        self.logger.debug(f"Fetching latest CSV file from {directory} with pattern {pattern}")
        try:
            csv_path = self.get_latest_file(directory, pattern)
            if os.path.getsize(csv_path) == 0:
                self.logger.debug(f"File {csv_path} is empty. Returning an empty DataFrame.")
                return pd.DataFrame()

            data = pd.read_csv(csv_path)
            if data.empty or data.columns.empty:
                self.logger.debug(f"File {csv_path} contains no data or no columns. Returning an empty DataFrame.")
                return pd.DataFrame()

            self.logger.debug(f"Loaded file: {csv_path}")
            return data
        except (pd.errors.EmptyDataError, FileNotFoundError) as e:
            self.logger.warning(f"File not found or is empty. Returning an empty DataFrame: {e}")
            return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Error loading CSV file from {directory}: {e}")
            return pd.DataFrame()

    def load_data_for_preparing_for_load_script(self, company_name):
        YA_BOOKINGS_DIR = 'data/raw/yango_bookings'
        MATCHED_DATA_DIR = 'data/processing/yango_cars'
        bookings_data = self.get_and_load_latest_csv(os.path.join(BASE_DIR, YA_BOOKINGS_DIR, company_name),
                                                     '*yango_bookings*.csv')
        matched_data = self.get_and_load_latest_csv(os.path.join(BASE_DIR, MATCHED_DATA_DIR, company_name),
                                                    '*_matched_*.csv')
        ya_unmatched_data = self.get_and_load_latest_csv(os.path.join(BASE_DIR, MATCHED_DATA_DIR, company_name),
                                                    '*_failed_*.csv')

        if bookings_data.empty:
            self.logger.warning(f"{company_name} Данные бронирований не найдены или пусты.")

        if ya_unmatched_data.empty:
            self.logger.warning(f"{company_name} Данные ya_unmatched_data не найдены или пусты.")

        if matched_data.empty:
            self.logger.warning(f"{company_name} Данные по мэтчам не найдены или пусты.")

        return bookings_data, matched_data, ya_unmatched_data



class JSONDataSaver:
    def __init__(self, logger):
        self.logger = logger

    def save_to_json(self, data, filename):
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        try:
            with open(filename, 'w', encoding='utf-8') as file:
                json.dump(data, file, indent=4)
            self.logger.debug(f"Data saved to {filename}")
        except IOError as e:
            self.logger.error(f"Failed to save data: {e}")


class DataNormalizer:
    @staticmethod
    def convert_nan_to_none(data):
        if isinstance(data, dict):
            return {k: DataNormalizer.convert_nan_to_none(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [DataNormalizer.convert_nan_to_none(item) for item in data]
        elif pd.isna(data):
            return None
        else:
            return data

    @staticmethod
    def extract_main_part_plate_no(plate_no):
        match = re.search(r'[a-zA-Z]?(\d+)[a-zA-Z]?', plate_no)
        if match:
            return match.group(0)
        return None

    @staticmethod
    def extract_number_part(plate_no):
        match = re.search(r'\d+', plate_no)
        if match:
            return match.group(0)
        return None

    @staticmethod
    def convert_to_json_format(text):
        if text:
            text = re.sub(r"'", '"', text)
            text = re.sub(r'None', 'null', text)
            text = re.sub(r'True', 'true', text)
            text = re.sub(r'False', 'false', text)
        return text

    @staticmethod
    def extract_letter_part(plate_no):
        match = re.findall(r'[a-zA-Z]', plate_no)
        if len(match) == 1:
            return match[0]
        elif len(match) == 2:
            return match[-1]
        else:
            return None

    @staticmethod
    def normalize_string(s):
        s = re.sub(r'\W+', '', str(s).lower())
        replacements = {
            'lamborgini': 'lamborghini', 'hurracan': 'huracan',
            'rollsroyse': 'rollsroyce', 'bentaga': 'bentayga',
            'hurcan': 'huracan', 'lambo': 'lamborghini',
            'chevrolete': 'chevrolet', 'culinnan': 'cullinan',
            'hundaisantafeh': 'hundaisantafe', 'posche': 'porsche',
            'cayean': 'cayenne', 'landroverrangrov': 'landroverrangerover'
        }
        for old, new in replacements.items():
            s = s.replace(old, new)
        return s


def safe_json_loads(x, logger):
    try:
        if pd.isna(x):
            return []
        x = DataNormalizer.convert_to_json_format(x)
        return json.loads(x)
    except json.JSONDecodeError as e:
        logger.error(f"Ошибка декодирования JSON: {x} — {e}")
        return []


def find_non_overlapping_intervals(new_intervals, existing_intervals, ya_id, logger):
    result_intervals = []

    for new_start, new_end, new_start_dubai, new_end_dubai in new_intervals:
        logger.debug(f"Processing new interval: [{new_start}, {new_end}] (Dubai: {new_start_dubai} - {new_end_dubai})")
        current_intervals = [(new_start, new_end, new_start_dubai, new_end_dubai)]

        for exist_start, exist_end in existing_intervals:
            logger.debug(f"Checking against existing interval: [{exist_start}, {exist_end}]")
            next_intervals = []
            for interval in current_intervals:
                interval_start, interval_end, interval_start_dubai, interval_end_dubai = interval
                if interval_end <= exist_start or interval_start >= exist_end:
                    next_intervals.append(interval)
                else:
                    logger.debug(f"Found overlapping interval with existing [{exist_start}, {exist_end}]")
                    if interval_start < exist_start:
                        next_intervals.append((interval_start, exist_start, interval_start_dubai,
                                    datetime.fromtimestamp(exist_start, TZ_DUBAI).strftime('%m/%d/%Y %I:%M:%S %p')))
                    if interval_end > exist_end:
                        next_intervals.append((exist_end, interval_end,
                                    datetime.fromtimestamp(exist_end, TZ_DUBAI).strftime('%m/%d/%Y %I:%M:%S %p'),
                                               interval_end_dubai))

            current_intervals = next_intervals
            logger.debug(f"Remaining intervals after comparison: {current_intervals}")

        result_intervals.extend(current_intervals)
        logger.debug(f"Non-overlapping intervals so far: {result_intervals}")

    logger.debug(f"Final resulting non-overlapping intervals fro {ya_id}: {result_intervals}")
    return result_intervals


def merge_overlapping_intervals(intervals):
    if not intervals:
        return []

    sorted_intervals = sorted(intervals, key=lambda x: x[0])
    merged_intervals = [sorted_intervals[0]]

    for current in sorted_intervals[1:]:
        last = merged_intervals[-1]
        if current[0] <= last[1]:
            merged_intervals[-1] = (last[0], max(last[1], current[1]), last[2], current[3])
        else:
            merged_intervals.append(current)
    return merged_intervals


def merge_matched_and_ya_unmatched_data(matched_data, ya_unmatched_data):
    unmatched_data = pd.DataFrame({
        'ya_id': ya_unmatched_data['id'],
        'ya_number': ya_unmatched_data['number'],
        'ya_merge_manufacturer': ya_unmatched_data['merge_manufacturer'],
        'ya_merge_short_name': ya_unmatched_data['merge_name'],
        'sheet_PlateNo': 'unmatched_data',
        'sheet_VehicleType': 'unmatched_data',
        'sheet_Status': 'hold_for_unmatched_data'
    })
    merged_data = pd.concat([matched_data, unmatched_data], ignore_index=True)

    return merged_data