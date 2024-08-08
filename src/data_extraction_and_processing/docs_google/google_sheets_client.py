import os
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from src.config import GoogleSheetsConfig, get_current_datetime, BASE_DIR
from src.settings import setup_logging
from src.data_helper import CSVDataSaver

GOOGLE_SHEETS_DIR = os.path.join(BASE_DIR, 'data/raw/docs_google')
os.makedirs(GOOGLE_SHEETS_DIR, exist_ok=True)


class GoogleSheetsClient:
    def __init__(self, auth_config, sheet_config, logger):
        self.auth_config = auth_config
        self.sheet_config = sheet_config
        self.logger = logger
        self.client = self.authenticate()

    def authenticate(self):
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(self.auth_config.credentials_file, scope)
        return gspread.authorize(creds)

    def get_data(self):
        try:
            spreadsheet = self.client.open(self.sheet_config['GOOGLE_SHEETS_NAME'])
            sheet = spreadsheet.worksheet(self.sheet_config['WORKSHEET_NAME'])
            data = sheet.get_all_values()
            self.logger.info("Data fetched successfully from Google Sheets.")
            return pd.DataFrame(data[1:], columns=data[0])
        except Exception as e:
            self.logger.error(f"Error fetching data: {e}")
            return None


def main(company_name, sheet_config):
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    logger = setup_logging(script_name)
    logger.debug("Script started.")

    auth_config = GoogleSheetsConfig()
    google_sheets_client = GoogleSheetsClient(auth_config, sheet_config, logger)
    data_saver = CSVDataSaver(logger)

    df = google_sheets_client.get_data()
    if df is not None:
        output_file = os.path.join(GOOGLE_SHEETS_DIR, company_name,
                                   f'{company_name}_data_{get_current_datetime()}.csv')
        data_saver.save_dataframe_to_csv(df, output_file)
    else:
        logger.error(f"{company_name} No data fetched from Google Sheets.")

    logger.info(f"{company_name} Script get google sheets finished.")


if __name__ == "__main__":
    company_name = "AL EMAD CAR RENTAL"
    sheet_cnfg = {
            "GOOGLE_SHEETS_NAME": "IMPORT Al Emad",
            "WORKSHEET_NAME": "Лист1"
            }
    main(company_name, sheet_cnfg)
