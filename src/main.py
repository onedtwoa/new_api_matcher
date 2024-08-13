import pytz
import os
from settings import setup_logging
from config import _config_json
from concurrent.futures import ThreadPoolExecutor
from src.data_extraction_and_processing.ya import ya_get_cars_and_bookings_data, ya_data_join, create_holds
from src.data_extraction_and_processing.takamol import takamol_get_car_bookings_data, \
                                        takamol_data_processing, takamol_data_matcher, takamol_prepare_for_loading
from src.data_extraction_and_processing.docs_google import google_sheets_client, google_sheets_data_matcher, \
                                        google_sheets_prepare_for_loading
from src.data_extraction_and_processing import del_old_data

pytz.timezone('Asia/Dubai')

script_name = os.path.splitext(os.path.basename(__file__))[0]
logger = setup_logging(script_name)

TAKAMOL_API_KEY = _config_json["TAKAMOL_API_KEY"]


def process_company(company_name, company_config):
    if company_name in ["AL EMAD CAR RENTAL", "CAR STREET CAR RENTAL"]:
        logger.info(f"<{company_name}> skip...")
        return
    logger.info(f"<{company_name}> start processing data...")
    token_drive_ya_tech = company_config['TOKEN_DRIVE_YA_TECH']
    tag_name = company_config['tag_name']
    takamol_member_no = company_config.get('TAKAMOL_MemberNo')
    config_google_sheets = company_config.get('config_google_sheets')
    try:
        logger.debug(f"<{company_name}> Получаем бронирования и данные по машинам с ya...")
        ya_get_cars_and_bookings_data.main(company_name, token_drive_ya_tech)
        ya_data_join.merge_csv_files(company_name)

        if takamol_member_no:
            logger.debug(f"<{company_name}> Получение данных по бронированиям с takamol...")
            takamol_get_car_bookings_data.main(company_name, takamol_member_no, TAKAMOL_API_KEY)

            logger.debug(f"<{company_name}> Убираем дубли с takamol и оставляем уникальные авто...")
            takamol_data_processing.main(company_name)

            logger.debug(f"<{company_name}> Выполняем мэтч takamol и ya...")
            takamol_data_matcher.main(company_name)

            logger.debug(f"<{company_name}> Выполняем подготовку к загрузке takamol и ya...")
            takamol_prepare_for_loading.main(company_name)
        elif config_google_sheets:
            logger.debug(f"<{company_name}> Получение данных по бронированиям с google_sheets...")
            google_sheets_client.main(company_name, config_google_sheets)

            logger.debug(f"<{company_name}> Выполняем мэтч google_sheets и ya...")
            google_sheets_data_matcher.main(company_name)

            logger.debug(f"<{company_name}> Выполняем подготовку к загрузке google_sheets и ya...")
            google_sheets_prepare_for_loading.main(company_name)
        else:
            logger.warning(f"<{company_name}> no data or processing method, "
                           f"check config - {company_config}")

        logger.info(f"<{company_name}> Ставим холды...")
        create_holds.main(company_name, token_drive_ya_tech, tag_name)

    except Exception as e:
        logger.error(f"<{company_name}> Error processing company: {e}")


def main():
    del_old_data.main()
    rantal_companies = _config_json['ya_companies']
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(process_company, company_name, company_config)
                   for company_name, company_config in rantal_companies.items()]

        for future in futures:
            future.result()


if __name__ == "__main__":
    main()

# переделать под текущие реалии скрипт для поиска авто с активной бронью которых у нас нет
# запихнуть все в докер и закинуть на сервер
# написать тг бота с сохранением файлов и статистикой
