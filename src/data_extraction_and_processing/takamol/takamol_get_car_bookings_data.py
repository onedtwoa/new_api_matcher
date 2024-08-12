import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from src.settings import setup_logging
from src.config import get_current_datetime, BASE_DIR
from src.data_helper import CSVDataSaver

API_BASE_URL = "http://www.takamol.com/api/TakamolMobileApi/CarsOnlineBooking_API"
DATA_DIR = os.path.join(BASE_DIR, 'data/raw/takamol')
os.makedirs(DATA_DIR, exist_ok=True)

script_name = os.path.splitext(os.path.basename(__file__))[0]
logger = setup_logging(script_name)


class TakamolAPIClient:
    def __init__(self, takamol_api_key, takamol_member_no):
        self.session = self.create_session()
        self.takamol_api_key = takamol_api_key
        self.takamol_member_no = takamol_member_no
        self.session.headers.update({'Authorization': f'Bearer {self.takamol_api_key}'})

    @staticmethod
    def create_session() -> requests.Session:
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        session = requests.Session()
        session.mount('http://', HTTPAdapter(max_retries=retries))
        return session

    def fetch_data(self, page_number: int, page_size: int):
        params = self.get_api_params(page_number, page_size)
        try:
            response = self.session.get(API_BASE_URL, params=params)
            logger.debug(f"Request URL: {response.url}, Status Code: {response.status_code}")
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch data: {e}")
            return None

    def get_api_params(self, page_number: int, page_size: int):
        return {
            "MemberAPIKey": self.takamol_api_key,
            "CountryNo": 0,
            "AreaNo": 0,
            "MemberNo": self.takamol_member_no,
            "CarName": "",
            "DailyPriceFrom": 0,
            "DailyPriceTo": 0,
            "ModelFrom": 0,
            "ModelTo": 0,
            "ReservationStatus": 0,
            "ReadCarPictures": 0,
            "ReadReservations": 1,
            "ReadReservationDocuments": 1,
            "Language": "E",
            "PageNumber": page_number,
            "PageSize": page_size
        }


class DataProcessor:
    def __init__(self, company_name):
        self.company_name = company_name
        self.all_cars = []

    def parse_data(self, json_data):
        if not json_data:
            return []

        cars = []
        for car in json_data:
            car_data = car.copy()
            reservations = car_data.pop('Reservations', [])
            car_data.update({'Reservations': reservations})
            cars.append(car_data)

        return cars


def fetch_all_data(client: TakamolAPIClient, processor: DataProcessor, page_size: int = 100) -> None:
    page_number = 1
    logger.debug(f"<{processor.company_name}> Start fetching takamol")
    while True:
        logger.debug(f"<{processor.company_name}> Fetching page {page_number}")
        json_data = client.fetch_data(page_number, page_size)
        if json_data:
            cars = processor.parse_data(json_data)
            if not cars:
                break
            processor.all_cars.extend(cars)
            page_number += 1
        else:
            break


def save_data_to_csv(processor: DataProcessor) -> None:
    filename = os.path.join(DATA_DIR, processor.company_name, f'takamol_cars_data_{get_current_datetime()}.csv')
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    if processor.all_cars:
        logger.info(f"<{processor.company_name}> {len(processor.all_cars)} cars fetched.")
        CSVDataSaver(logger).save_dict_to_csv(processor.all_cars, filename)
    else:
        logger.info(f"<{processor.company_name}> No data fetched.")


def main(company_name, takamol_member_no, takamol_api_key) -> None:
    client = TakamolAPIClient(takamol_api_key, takamol_member_no)
    processor = DataProcessor(company_name)
    fetch_all_data(client, processor)
    save_data_to_csv(processor)


if __name__ == "__main__":
    from src.main import TAKAMOL_API_KEY
    company_name = "ROTANA STAR RENT A CAR"
    takamol_member_no = 2114
    main(company_name, takamol_member_no, TAKAMOL_API_KEY)
