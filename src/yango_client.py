import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging


class YangoAPIClient:
    def __init__(self, base_url: str, token: str, logger: logging.Logger):
        self.base_url = base_url
        self.session = self.create_session(token)
        self.logger = logger

    def create_session(self, token: str):
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        session = requests.Session()
        session.mount('http://', HTTPAdapter(max_retries=retries))
        session.headers.update({'Authorization': f'Bearer {token}'})
        return session

    def fetch_bookings(self, endpoint: str, params=None):
        url = f"{self.base_url}/{endpoint}"
        try:
            response = self.session.post(url, json=params)
            self.logger.debug(f"Request URL: {response.url}, Status Code: {response.status_code}")
            response.raise_for_status()
            offers_timetable = response.json().get('offers_timetable', {})
            bookings = []
            for id_car, sublist in offers_timetable.items():
                for item in sublist:
                    item.update({'id_car': id_car})
                    bookings.append(item)
            self.logger.debug(f"Fetched {len(bookings)} bookings")
            return bookings
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to fetch bookings: {e}")
            return []

    def fetch_model_list(self, endpoint: str):
        url = f"{self.base_url}/{endpoint}?lang=en"
        try:
            response = self.session.get(url)
            self.logger.debug(f"Request URL: {response.url}, Status Code: {response.status_code}")
            response.raise_for_status()
            return response.json().get('models', [])
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to fetch model list: {e}")
            return []

    def fetch_all_cars_with_pagination(self, endpoint: str, initial_params=None):
        if initial_params is None:
            initial_params = {
                "page_size": 50,
                "lang": "en"
            }

        all_cars = []
        params = initial_params.copy()
        page_number = 1
        self.logger.debug(f"start fetch_all_cars_with_pagination")
        while True:
            params['page_number'] = page_number
            url_with_params = f"{self.base_url}/{endpoint}?page_number={params['page_number']}" \
                              f"&page_size={params['page_size']}&lang={params['lang']}"
            try:
                response = self.session.post(url_with_params)
                self.logger.debug(f"Request URL: {response.url}, Status Code: {response.status_code}")
                response.raise_for_status()
                cars = response.json().get('cars', [])
                if not cars:
                    break
                self.logger.debug(f"number of cars-{len(cars)} in page={page_number}")
                all_cars.extend(cars)
                page_number += 1
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Failed to fetch data for page {page_number}: {e}")
                break

        return all_cars

    def add_hold_car(self, endpoint: str, params=None, string_params=None):
        url = f"{self.base_url}/{endpoint}"
        if string_params:
            from urllib.parse import urlencode
            url = f"{url}?{urlencode(string_params)}"
        try:
            response = self.session.post(url, json=params)
            self.logger.debug(f"Request URL: {response.url}, Status Code: {response.status_code}")
            if response.status_code == 200:
                self.logger.info("Tag successfully added to car")
            elif response.status_code == 409:
                self.logger.warning("Processing existing records")
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to add tag: {e}")
            return None
