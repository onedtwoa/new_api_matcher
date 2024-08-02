import pandas as pd
import os
from src.settings import setup_logging
from src.config import get_current_datetime, BASE_DIR
from src.data_helper import LatestFileFetcher, CSVDataSaver,\
    DataNormalizer, JSONDataSaver

script_name = os.path.splitext(os.path.basename(__file__))[0]
logger = setup_logging(script_name)
file_fetcher = LatestFileFetcher(logger)
data_saver = CSVDataSaver(logger)
json_saver = JSONDataSaver(logger)

YA_DIR = 'data/processing/yango_cars'
GOOGLE_SHEETS_DIR = 'data/raw/docs_google'


def load_data(company_name):
    sheet_data = file_fetcher.get_and_load_latest_csv(os.path.join(BASE_DIR, GOOGLE_SHEETS_DIR,
                                                                   company_name), '*_data*.csv')
    yango_cars_data = file_fetcher.get_and_load_latest_csv(os.path.join(BASE_DIR, YA_DIR,
                                                                        company_name), '*merged_yango_data*.csv')
    return sheet_data, yango_cars_data


def create_match_record(sheet_row, match_row):
    return {
        "ya_id": match_row['id'],
        "ya_number": match_row['number'],
        "ya_merge_manufacturer": match_row['merge_manufacturer'],
        "ya_merge_short_name": match_row['merge_short_name'],
        "sheet_PlateNo": sheet_row['Plate No'],
        "sheet_VehicleType": sheet_row['Vehicle Type'],
        "sheet_Status": sheet_row['Status']
    }


def add_to_multiple_matches(multiple_matches, sheet_row, matches):
    multiple_matches.append({
        "plate_no": sheet_row['Plate No'],
        "sheet_row": DataNormalizer.convert_nan_to_none(sheet_row.to_dict()),
        "matches": DataNormalizer.convert_nan_to_none(matches.to_dict('records'))
    })


def match_cars(sheet_data, yango_cars_data):
    matched = []
    failed_sheet = []
    failed_yango = yango_cars_data.copy()
    multiple_matches = []

    for _, sheet_row in sheet_data.iterrows():
        plate_no = DataNormalizer.normalize_string(sheet_row['Plate No'])
        main_part_plate_no = DataNormalizer.extract_main_part_plate_no(plate_no)

        if not main_part_plate_no:
            failed_sheet.append(sheet_row)
            continue

        matches = yango_cars_data[
            yango_cars_data['number'].apply(lambda x: main_part_plate_no in DataNormalizer.normalize_string(x))]

        if len(matches) == 1:
            matched.append(create_match_record(sheet_row, matches.iloc[0]))
            failed_yango = failed_yango[failed_yango['number'] != matches.iloc[0]['number']]
        elif len(matches) > 1:
            vehicle_type = DataNormalizer.normalize_string(sheet_row['Vehicle Type'])
            manufacturer_matches = matches[
                matches['merge_manufacturer'].apply(
                    lambda x: DataNormalizer.normalize_string(x) in vehicle_type)
            ]

            if len(manufacturer_matches) == 1:
                matched.append(create_match_record(sheet_row, manufacturer_matches.iloc[0]))
                failed_yango = failed_yango[failed_yango['number'] != manufacturer_matches.iloc[0]['number']]
            else:
                add_to_multiple_matches(multiple_matches, sheet_row, matches)
                failed_sheet.append(sheet_row)
        else:
            failed_sheet.append(sheet_row)

    matched_df = pd.DataFrame(matched)
    return matched_df, failed_sheet, failed_yango, multiple_matches



def main(company_name):
    sheet_data, yango_cars_data = load_data(company_name)
    matched, failed_sheet, failed_yango, multiple_matches = match_cars(sheet_data, yango_cars_data)

    logger.info(f"{company_name} Total cars in Google Sheets: {len(sheet_data)}")
    logger.info(f"{company_name} Total cars in YA: {len(yango_cars_data)}")
    logger.info(f"{company_name} Successfully matched cars: {len(matched)}")
    logger.info(f"{company_name} Multiple matches cars: {len(multiple_matches)}")
    logger.info(f"{company_name} Unsuccessfully matched cars from Google Sheets: {len(failed_sheet)}")
    logger.info(f"{company_name} Unsuccessfully matched cars from YA: {len(failed_yango)}")

    full_yango_dir = os.path.join(BASE_DIR, YA_DIR, company_name)

    if not matched.empty:
        data_saver.save_dataframe_to_csv(matched, os.path.join(full_yango_dir,
                                        f"{company_name}_matched_{get_current_datetime()}.csv"))

    if not failed_yango.empty:
        data_saver.save_dataframe_to_csv(failed_yango, os.path.join(full_yango_dir,
                                        f"{company_name}_failed_yango_{get_current_datetime()}.csv"))
    if multiple_matches:
        json_saver.save_to_json(multiple_matches, os.path.join(full_yango_dir,
                                        f'{company_name}_multiple_matches_{get_current_datetime()}.json'))

    logger.info("Script finished successfully")


if __name__ == "__main__":
    company_name = "CAR STREET CAR RENTAL"
    main(company_name)
