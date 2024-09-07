import re
import pandas as pd
import os
import xmltodict
from decimal import Decimal
from utilities.extract import archive_raw_file
import shutil
from airflow.utils.log.logging_mixin import LoggingMixin
from utilities.params import get_param_values
from datetime import datetime, time

EXEMPLARY_PRESSURE = 1013.25


RAW_FILES_FOLDER, TRANSFORMED_FILES_FOLDER, LOADED_FILES_FOLDER = get_param_values(['raw_files_folder',
                                                                                    'transformed_files_folder',
                                                                                    'loaded_files_folder'])

logger = LoggingMixin().log


def add_exemplary_pressure_difference(df):
    """Dodaje dla każdego wiersza kolumnę z wartością różnicy
    pomiędzy ciśnieniem w danych z API, a ciśnieniem wzorcowym
    """
    try:
        df['roznica_od_wzorcowego'] = df['cisnienie'].apply(
                                                        lambda row:
                                                        # Decimal w celu uniknięcia późniejszych problemów z
                                                        # operacjami arytmetycznymi na liczbach z częścią dziesiętną
                                                        Decimal(row) - Decimal(EXEMPLARY_PRESSURE) if row is not None
                                                        else None)
        logger.info(f"Calculating pressure difference done successfully")
        return df
    except Exception as e:
        logger.error(f"Error while calculating pressure difference: {e}")
        raise

def add_datetime(df):
    try:
        df['data_czas'] = df.apply(lambda row: convert_to_datetime(row.data_pomiaru, row.godzina_pomiaru), axis=1)
        logger.info(f"Calculating pressure difference done successfully")
        return df
    except Exception as e:
        logger.error(f"Error while calculating pressure difference: {e}")
        raise


def convert_to_datetime(date, time_str):
    """
    Funkcja do konwersji daty i czasu podanego w parametrach do postaci datetime
    """
    date = datetime.strptime(date, '%Y-%m-%d').date()
    mytime = time(int(time_str), 0, 0)
    mydatetime = datetime.combine(date, mytime)
    return mydatetime


def save_transformed_data_to_files():
    source_filename_pattern = re.compile(r"^RawData_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}.xml$")
    for source_filename in os.listdir(RAW_FILES_FOLDER):
        if source_filename_pattern.match(source_filename):
            logger.info(f"Transformation and saving process for raw XML. Processing file: {source_filename}")
            try:
                destination_filename = (source_filename
                                        .replace("RawData", "Transformed")
                                        .replace(".xml", ".csv"))
                transformed_data = transform_data_from_file(source_filename)
                transformed_data.to_csv(f"{TRANSFORMED_FILES_FOLDER}/{destination_filename}", sep=',', index=False)
                logger.info(f"Transformed data saved to: {TRANSFORMED_FILES_FOLDER}/{destination_filename}")
                archive_raw_file(source_filename)
                logger.info(f"Raw file {source_filename} archived successfully.")
            except Exception as e:
                logger.error(f"Error processing file {source_filename}: {e}")
                raise


def transform_data_from_file(file_name):
    """
    Wykonuje wszystkie transformację na 'surowych' danych z API
    """
    raw_filepath = os.path.join(RAW_FILES_FOLDER, file_name)

    with open(raw_filepath, "r") as f:
        data = f.read()
    try:
        logger.info(f"Transforming data from file: {file_name}")
        dict_data = xmltodict.parse(data)
        transformed_data = pd.DataFrame(dict_data['xml']['item'])
        transformed_data = add_exemplary_pressure_difference(transformed_data)
        transformed_data = add_datetime(transformed_data)
        return transformed_data
    except Exception as e:
        logger.error(f"Error while transforming data from file {file_name}: {e}")
        raise


def move_transformed_file_to_loaded(file_name):
    """Funkcja odpowiedzialna za przeniesienie plików z
    folderu, gdzie zapisujemy pliki po transformacjach do
    folderu z zaimportowanymi już plikami do bazy
    """
    transformed_filepath = os.path.join(TRANSFORMED_FILES_FOLDER, file_name)
    loaded_filepath = os.path.join(LOADED_FILES_FOLDER, file_name)
    try:
        shutil.move(transformed_filepath, loaded_filepath)
        logger.info(f"File {file_name} successfully moved to {LOADED_FILES_FOLDER}")
    except Exception as e:
        logger.error(f"Error while moving file {file_name} to {LOADED_FILES_FOLDER}: {e}")
        raise
