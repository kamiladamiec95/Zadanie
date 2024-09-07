from requests import get
from datetime import datetime
from pytz import timezone
import shutil
from airflow.utils.log.logging_mixin import LoggingMixin
from utilities.params import get_param_values
import os


API_URL, RAW_FILES_FOLDER, ARCHIVE_RAW_FILES_FOLDER = get_param_values(['api_url',
                                                                        'raw_files_folder',
                                                                        'archive_raw_files_folder'])

logger = LoggingMixin().log


def save_raw_data_to_file(data):
    """
    Funkcja odpowiedzialna za zapis danych do pliku
    w katalogu którego nazwa jest przechowywana w 'stałej' DESTINATION_FILES_FOLDER.
    Nazwa pliku jest budowana w postaci RawData_{aktualna data i godzina w Polsce}.
    """
    poland_timezone = timezone('Europe/Warsaw')
    current_datetime_poland = datetime.now(poland_timezone).strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"RawData_{current_datetime_poland}.xml"
    raw_filepath = os.path.join(RAW_FILES_FOLDER, filename)

    with open(raw_filepath, "w") as f:
        f.write(data)

    return filename


def get_xml_data_from_api_to_file():
    """
    Funkcja odpowiedzialna za pobranie danych z API,
    następnie zapis do pliku
    """
    try:
        response_api = get(API_URL)
        response_api.raise_for_status()

        api_data = response_api.text

        filename = save_raw_data_to_file(api_data)

        logger.info(f"API data successfully fetched and saved to {filename}")

    except Exception as e:
        logger.error(f"Error fetching data from API: {e}")
        raise


def archive_raw_file(filename):
    """
    Funkcja odpowiedzialna za archiwizacje plików z
    folderu, gdzie zapisujemy pliki z API do folderu z archiwalnymi plikami
    """
    raw_filepath = os.path.join(RAW_FILES_FOLDER, filename)
    archive_filepath = os.path.join(ARCHIVE_RAW_FILES_FOLDER, filename)
    shutil.move(raw_filepath, archive_filepath)
