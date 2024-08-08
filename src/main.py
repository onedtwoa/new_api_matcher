import pytz
import os
from settings import setup_logging
from config import _config_json
from concurrent.futures import ThreadPoolExecutor
from src.data_extraction_and_processing.ya import ya_get_cars_and_bookings_data, ya_data_join
from src.data_extraction_and_processing.takamol import takamol_get_car_bookings_data, \
    takamol_data_processing, takamol_data_matcher, prepare_for_loading
from src.data_extraction_and_processing.docs_google import google_sheets_client, google_sheets_data_matcher

pytz.timezone('Asia/Dubai')

script_name = os.path.splitext(os.path.basename(__file__))[0]
logger = setup_logging(script_name)

TAKAMOL_API_KEY = _config_json["TAKAMOL_API_KEY"]


def process_company(company_name, company_config):
    logger.info(f"start processing data for {company_name}...")
    token_drive_ya_tech = company_config['TOKEN_DRIVE_YA_TECH']
    tag_name = company_config['tag_name']
    takamol_member_no = company_config.get('TAKAMOL_MemberNo')
    config_google_sheets = company_config.get('config_google_sheets')
    try:
        # 1. Получаем данные
        logger.info(f"{company_name}: Получаем бронирования и данные по машинам с ya...")
        ya_get_cars_and_bookings_data.main(company_name, token_drive_ya_tech)
        ya_data_join.merge_csv_files(company_name)

        if takamol_member_no:

            logger.info(f"{company_name}: Получение данных по бронированиям с takamol...")
            takamol_get_car_bookings_data.main(company_name, takamol_member_no, TAKAMOL_API_KEY)

            logger.info(f"{company_name}: Убираем дубли с takamol и оставляем уникальные авто...")
            takamol_data_processing.main(company_name)

            logger.info(f"{company_name}: Выполняем мэтч takamol и ya...")
            takamol_data_matcher.main(company_name)

            logger.info(f"{company_name}: Выполняем подготовку к загрузке takamol и ya...")
            prepare_for_loading.main(company_name)
        elif config_google_sheets:

            logger.info(f"{company_name}: Получение данных по бронированиям с google_sheets...")
            google_sheets_client.main(company_name, config_google_sheets)

            logger.info(f"{company_name}: Выполняем мэтч google_sheets и ya...")
            google_sheets_data_matcher.main(company_name)
        else:

            logger.warning(f"no data or processing method for company - {company_name}, "
                           f"check config - {company_config}")

    except Exception as e:
        logger.error(f"Error processing company {company_name}: {e}")


def main():
    # logger.info("Запуск очистки старых файлов...")
    # del_old_data_logs.main()

    rantal_companies = _config_json['ya_companies']
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(process_company, company_name, company_config)
                   for company_name, company_config in rantal_companies.items()]

        for future in futures:
            future.result()

    # active_unmatched_cars.main()


if __name__ == "__main__":
    main()
