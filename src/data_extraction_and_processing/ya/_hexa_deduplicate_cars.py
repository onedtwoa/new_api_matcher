import os
import pandas as pd
from src.settings import setup_logging
from src.config import get_current_datetime, BASE_DIR
from src.data_helper import LatestFileFetcher

script_name = os.path.splitext(os.path.basename(__file__))[0]
logger = setup_logging(script_name)
file_fetcher = LatestFileFetcher(logger)

CARS_DIR = 'data/raw/yango_cars'
BOOKINGS_INPUT_DIR = 'data/raw/yango_bookings'


def load_latest_files(company_name):
    cars_input_dir = os.path.join(BASE_DIR, CARS_DIR, company_name)
    bookings_input_dir = os.path.join(BASE_DIR, BOOKINGS_INPUT_DIR, company_name)

    cars_data = file_fetcher.get_and_load_latest_csv(cars_input_dir, '*yango_cars*.csv')
    bookings_data = file_fetcher.get_and_load_latest_csv(bookings_input_dir, '*yango_bookings*.csv')

    bookings_data = bookings_data[bookings_data['status_title'] != 'rental.status.on_hold.title']

    return cars_data, bookings_data


def process_duplicates(duplicates, bookings_data):
    unique_cars = []
    complex_cases = []
    remaining_duplicates = []

    for _, group in duplicates.groupby(['model_id', 'number']):
        relevant_bookings = bookings_data[bookings_data['id_car'].isin(group['id'])]

        if not relevant_bookings.empty:
            booked_cars = group[group['id'].isin(relevant_bookings['id_car'])]
            if len(booked_cars) == 1:
                unique_cars.append(booked_cars.iloc[0])
                remaining_duplicates.extend(group.drop(booked_cars.index).to_dict('records'))
            elif len(booked_cars) > 1:
                complex_cases.append(booked_cars)
                remaining_duplicates.extend(group.drop(booked_cars.index).to_dict('records'))
        else:
            unique_cars.append(group.iloc[0])
            remaining_duplicates.extend(group.iloc[1:].to_dict('records'))

    unique_cars_df = pd.DataFrame(unique_cars)
    complex_cases_df = pd.concat(complex_cases) if complex_cases else pd.DataFrame()
    remaining_duplicates_df = pd.DataFrame(remaining_duplicates)

    return unique_cars_df, complex_cases_df, remaining_duplicates_df


def save_results(unique_cars, complex_cases, duplicates, company_name):
    output_dir = os.path.join(BASE_DIR, CARS_DIR, company_name)
    os.makedirs(output_dir, exist_ok=True)

    unique_cars_file = os.path.join(output_dir, f'yango_unique_cars_{get_current_datetime()}.csv')
    complex_cases_file = os.path.join(output_dir, f'yango_complex_cases_{get_current_datetime()}.csv')
    duplicates_file = os.path.join(output_dir, f'yango_duplicates_{get_current_datetime()}.csv')

    unique_cars.to_csv(unique_cars_file, index=False)
    complex_cases.to_csv(complex_cases_file, index=False)
    duplicates.to_csv(duplicates_file, index=False)

    logger.info(f"Unique cars saved to: {unique_cars_file}")
    logger.info(f"Complex cases saved to: {complex_cases_file}")
    logger.info(f"Duplicates saved to: {duplicates_file}")


def main(company_name):
    cars_data, bookings_data = load_latest_files(company_name)

    grouped_data = cars_data.groupby(['model_id', 'number']).size().reset_index(name='count')

    multiple_records = grouped_data[grouped_data['count'] > 1]
    duplicate_keys = multiple_records[['model_id', 'number']]
    duplicates = cars_data.merge(duplicate_keys, on=['model_id', 'number'], how='inner')

    unique_cars = cars_data.drop_duplicates(subset=['model_id', 'number'], keep=False)

    validated_cars, complex_cases, remaining_duplicates = process_duplicates(duplicates, bookings_data)

    validated_cars = pd.concat([validated_cars, unique_cars])

    save_results(validated_cars, complex_cases, remaining_duplicates, company_name)


if __name__ == "__main__":
    company_name = "HEXA CAR RENTAL"
    main(company_name)


# убирать дубли  всегда!!