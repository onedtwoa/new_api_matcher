import os
from src.settings import setup_logging
from src.config import get_current_datetime, BASE_DIR
from src.data_helper import LatestFileFetcher

script_name = os.path.splitext(os.path.basename(__file__))[0]
logger = setup_logging(script_name)
file_fetcher = LatestFileFetcher(logger)
OUTPUT_DIR = 'data/processing/takamol'
INPUT_DIR = 'data/raw/takamol'


def main(company_name):
    input_data_dir = os.path.join(BASE_DIR, INPUT_DIR, company_name)
    takamol_data = file_fetcher.get_and_load_latest_csv(input_data_dir, '*takamol_cars*.csv')

    # Группировка данных по CarName, CarNo и Model и подсчет количества записей для каждой группы
    grouped_data = takamol_data.groupby(['CarName', 'CarNo', 'Model']).size().reset_index(name='count')

    # Фильтрация групп с количеством записей больше 1
    multiple_records = grouped_data[grouped_data['count'] > 1]

    # Получение записей с дублирующимися CarName, CarNo и Model
    duplicate_keys = multiple_records[['CarName', 'CarNo', 'Model']]
    duplicates = takamol_data.merge(duplicate_keys, on=['CarName', 'CarNo', 'Model'], how='inner')

    # Получение записей без дублирующихся CarName, CarNo и Model
    unique = takamol_data.merge(duplicate_keys, on=['CarName', 'CarNo', 'Model'], how='outer', indicator=True)
    unique = unique[unique['_merge'] == 'left_only'].drop(columns=['_merge'])

    # Создание директорий для уникальных и дублирующихся записей
    unique_output_dir = os.path.join(BASE_DIR, OUTPUT_DIR, company_name)
    duplicates_output_dir = os.path.join(BASE_DIR, OUTPUT_DIR, company_name)
    os.makedirs(unique_output_dir, exist_ok=True)
    os.makedirs(duplicates_output_dir, exist_ok=True)

    # Сохранение уникальных записей в новый файл
    unique_output_file = os.path.join(unique_output_dir, f'takamol_unique_cars_{get_current_datetime()}.csv')
    unique.to_csv(unique_output_file, index=False)
    logger.info(f"Сохранены уникальные записи в файл: {unique_output_file}")

    # Сохранение дублирующихся записей в новый файл
    duplicates_output_file = os.path.join(duplicates_output_dir, f'takamol_duplicate_cars_{get_current_datetime()}.csv')
    duplicates.to_csv(duplicates_output_file, index=False)
    logger.info(f"Сохранены дублирующиеся записи в файл: {duplicates_output_file}")


if __name__ == "__main__":
    company_name = "ROTANA STAR RENT A CAR"
    main(company_name)
