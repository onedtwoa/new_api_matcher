import os
import csv
import pandas as pd
import glob
import json
import re


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
            self.logger.error(f"Error getting the latest file: {e}")
            raise

    def get_and_load_latest_json(self, directory, pattern):
        self.logger.debug(f"Fetching latest JSON file from {directory} with pattern {pattern}")
        try:
            json_path = self.get_latest_file(directory, pattern)
            with open(json_path, mode='r', encoding='utf-8') as json_file:
                data = json.load(json_file)
            self.logger.debug(f"Loaded file: {json_path}")
            return data
        except Exception as e:
            self.logger.error(f"Error loading file from {directory}: {e}")
            raise

    def get_and_load_latest_csv(self, directory, pattern):
        self.logger.debug(f"Fetching latest CSV file from {directory} with pattern {pattern}")
        try:
            csv_path = self.get_latest_file(directory, pattern)
            if os.path.getsize(csv_path) == 0:
                self.logger.debug(f"File {csv_path} is empty. Returning an empty DataFrame.")
                return pd.DataFrame()

            try:
                data = pd.read_csv(csv_path)
                if data.empty or data.columns.empty:
                    self.logger.debug(f"File {csv_path} contains no data or no columns. Returning an empty DataFrame.")
                    return pd.DataFrame()
            except pd.errors.EmptyDataError:
                self.logger.debug(f"File {csv_path} is completely empty. Returning an empty DataFrame.")
                return pd.DataFrame()

            self.logger.debug(f"Loaded file: {csv_path}")
            return data
        except Exception as e:
            self.logger.error(f"Error loading file from {directory}: {e}")
            raise

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
