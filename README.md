### Проект по автоматизации работы с бронированиями авто


Этот проект автоматизирует процесс получения данных о бронированиях 
авто из источников Google Sheets, Takamol.,
их обработки, мэтчинга и постановки холдов.

## Содержание

1. [Настройка Google Sheets](#настройка-google-sheets)
2. [Работа с данными](#работа-с-данными)
   1. [Извлечение данных](#извлечение-данных)

## Настройка Google Sheets

### Краткое описание необходимых действий перед работой с Google Sheets

1. **Создание проекта в Google Cloud Console:**
   - Создаем новый проект [Google Cloud Console](https://console.cloud.google.com/).

2. **Включение API для Google Sheets и Google Drive:**
   - "API и сервисы" > "Библиотека" > Включаем "Google Sheets API" и "Google Drive API"

3. **Создание учетных данных для сервисного аккаунта:**
   - В меню "API и сервисы" > "Учетные данные".
   - "Создать учетные данные" и создаем "Сервисный аккаунт" (оставляем все по умолчанию)
   - Далее в акке на вкладке "Создание ключа" выбераем "Создать ключ" в формате JSON (наш credentials_file)

4. **Предоставление доступа к Google Sheets:**
   - Настройки доступа - email сервисного аккаунта из JSON файла

5. **IMPORTRANGE**
   - Импортирует диапазон ячеек из одной электронной таблицы в другую. Пример:      
   `=IMPORTRANGE("https://docs.google.com/spreadsheets/d/.../edit"; "World Cup!A1:D21")`


## Работа с данными

#### config.py

#### settings.py

`setup_logging` - для определения параметров логирования
(уровни логирования, формат логов и пути к файлам логов)

### Извлечение данных
 `data/raw`
#### google_sheets_client.py
   `main(company_name, sheet_config)` 
#### takamol_get_car_bookings_data.py
   `main(company_name, takamol_member_no, TAKAMOL_API_KEY)` 
#### ya_get_cars_and_bookings_data.py
   `main(company_name, token_drive_ya_tech)` 
