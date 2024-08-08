import json
import pandas as pd
import os
import re
from src.settings import setup_logging
from src.config import get_current_datetime, BASE_DIR
from src.data_helper import LatestFileFetcher,\
    CSVDataSaver, DataNormalizer, JSONDataSaver

script_name = os.path.splitext(os.path.basename(__file__))[0]
logger = setup_logging(script_name)
file_fetcher = LatestFileFetcher(logger)
data_saver = CSVDataSaver(logger)
json_saver = JSONDataSaver(logger)

YA_DIR = 'data/processing/yango_cars'
TAKAMOL_DIR = 'data/processing/takamol'


def load_data(company_name):
    takamol_data = file_fetcher.get_and_load_latest_csv(os.path.join(BASE_DIR, TAKAMOL_DIR,
                                                                    company_name), '*takamol_unique_cars*.csv')
    yango_cars_data = file_fetcher.get_and_load_latest_csv(os.path.join(BASE_DIR, YA_DIR,
                                                                    company_name), '*merged_yango_data*.csv')
    return takamol_data, yango_cars_data


def extract_year_from_specifications(specs):
    try:
        specs_list = json.loads(specs.replace("'", '"'))
        for spec in specs_list:
            if spec.get('name') == 'Year' and 'value' in spec:
                return int(spec['value'])
    except json.JSONDecodeError:
        return None
    return None


def add_to_multiple_matches(multiple_matches, takamol_row, matches):
    multiple_matches.append({
        "number": takamol_row['CarNo'],
        "takamol_row": DataNormalizer.convert_nan_to_none(takamol_row.to_dict()),
        "matches": DataNormalizer.convert_nan_to_none(matches.to_dict('records'))
    })


def create_match_record(takamol_row, match_row):
    return {
        "ya_id": match_row['id'],
        "ya_number": match_row['number'],
        "ya_merge_manufacturer": match_row['merge_manufacturer'],
        "ya_merge_short_name": match_row['merge_short_name'],
        "takamol_CarNo": takamol_row['CarNo'],
        "takamol_Model": takamol_row['Model'],
        "takamol_MemberNo": takamol_row['MemberNo'],
        "takamol_CarKey": takamol_row['CarKey'],
        "takamol_CarName": takamol_row['CarName'],
        "takamol_Reservations": takamol_row['Reservations']
    }


def match_cars(takamol_data, yango_cars_data):
    matched = []
    failed_takamol = []
    failed_yango = yango_cars_data.copy()
    multiple_matches = []

    for _, takamol_row in takamol_data.iterrows():
        car_no = DataNormalizer.normalize_string(takamol_row['CarNo'])
        car_name = DataNormalizer.normalize_string(takamol_row['CarName'])
        model_year = takamol_row['Model']
        matches = yango_cars_data[
            yango_cars_data['number'].apply(
                lambda x: DataNormalizer.normalize_string(car_no) in DataNormalizer.normalize_string(x) or
                          DataNormalizer.normalize_string(x) in DataNormalizer.normalize_string(car_no)) &
            yango_cars_data['merge_manufacturer'].apply(lambda x: DataNormalizer.normalize_string(x)
                                                                  in DataNormalizer.normalize_string(car_name))]
        if len(matches) == 1:
            matched.append(create_match_record(takamol_row, matches.iloc[0]))
            failed_yango = failed_yango[failed_yango['number'] != matches.iloc[0]['number']]
        elif len(matches) > 1:
            if pd.notna(model_year) and re.match(r'^\d{4}$', str(model_year)):
                model_year_matches = [match for _, match in matches.iterrows()
                            if extract_year_from_specifications(match.get('model_specifications_x')) == model_year]
                if len(model_year_matches) == 1:
                    matched.append(create_match_record(takamol_row, model_year_matches[0]))
                    failed_yango = failed_yango[failed_yango['number'] != model_year_matches[0]['number']]
                    continue
            add_to_multiple_matches(multiple_matches, takamol_row, matches)
            failed_takamol.append(takamol_row)
        else:
            failed_takamol.append(takamol_row)

    matched_df = pd.DataFrame(matched)
    return matched_df, failed_takamol, failed_yango, multiple_matches


def main(company_name):
    takamol_data, yango_cars_data = load_data(company_name)
    matched, failed_takamol, failed_yango, multiple_matches = match_cars(takamol_data, yango_cars_data)

    logger.info(f"{company_name} Total cars in Takamol: {len(takamol_data)}")
    logger.info(f"{company_name} Total cars in YA: {len(yango_cars_data)}")
    logger.info(f"{company_name} Successfully matched cars: {len(matched)}")
    logger.info(f"{company_name} Multiple matches cars: {len(multiple_matches)}")
    logger.info(f"{company_name} Unsuccessfully matched cars from Takamol: {len(failed_takamol)}")
    logger.info(f"{company_name} Unsuccessfully matched cars from YA: {len(failed_yango)}")

    full_yango_dir = os.path.join(BASE_DIR, YA_DIR, company_name)

    if not matched.empty:
        data_saver.save_dataframe_to_csv(matched, os.path.join(full_yango_dir,
                                        f"{company_name}_matched_{get_current_datetime()}.csv"))
    if multiple_matches:
        json_saver.save_to_json(multiple_matches, os.path.join(full_yango_dir,
                                        f'{company_name}_multiple_matches_{get_current_datetime()}.json'))

    logger.info(f"{company_name} Script finished successfully")


if __name__ == "__main__":
    company_name = "ROTANA STAR RENT A CAR"
    main(company_name)
