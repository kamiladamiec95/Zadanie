from datetime import datetime, time
from utilities.transform import move_transformed_file_to_loaded
from utilities.connection import create_conn_engine
from utilities.params import get_param_values, change_last_import_date
from airflow.utils.log.logging_mixin import LoggingMixin
import pandas as pd
import re
import os
import shutil


TRANSFORMED_FILES_FOLDER, ERROR_FILES_FOLDER = get_param_values(['transformed_files_folder', 'error_files_folder'])

logger = LoggingMixin().log

# logger = logging.getLogger(__name__)
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def load_data_to_sql():
    source_filename_pattern = re.compile(r"^Transformed_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}.csv$")
    # Posortowane alfabetycznie pliki ze względnu na to, że może być kilka plików z różnych dat
    # i do poprawnego działania należy zacząć od najwcześniejszego
    for source_filename in sorted(os.listdir(TRANSFORMED_FILES_FOLDER)):
        # Tylko dla plików o nazwie spełniającej określony wzorzec
        if source_filename_pattern.match(source_filename):
            try:
                logger.info(f"Processing file: {source_filename}")
                last_import_date, = get_param_values(['last_import_date'])
                last_import_date = datetime.strptime(last_import_date, '%Y-%m-%d %H:%M:%S')
                transformed_filepath = os.path.join(TRANSFORMED_FILES_FOLDER, source_filename)
                data = pd.read_csv(transformed_filepath)
                logger.info(f"Data read from file {source_filename}. Checking load conditions...")
                if df_datetime := check_load_conditions(data, last_import_date):
                    engine = create_conn_engine("localhost_mssql") # ???
                    data.to_sql('POGODA_W_POLSCE', con=engine, if_exists='append', index=False)
                    logger.info(f"Data successfully loaded into SQL table from file {source_filename}.")
                    change_last_import_date(str(df_datetime))
                move_transformed_file_to_loaded(source_filename)
                logger.info(f"File {source_filename} moved to the 'loaded' folder.")
            except Exception as e:
                transformed_filepath = os.path.join(TRANSFORMED_FILES_FOLDER, source_filename)
                error_filepath = os.path.join(ERROR_FILES_FOLDER, source_filename)
                shutil.move(transformed_filepath, error_filepath)
                logger.error(f"Error while processing file {source_filename}: {e}")
                raise




# validations
def convert_datetime(date, time_str):
    """
    Funkcja do konwersji daty i czasu podanego w parametrach do postaci datetime
    """
    date = datetime.strptime(date, '%Y-%m-%d').date()
    mytime = time(int(time_str), 0, 0)
    mydatetime = datetime.combine(date, mytime)
    return mydatetime

# skrypt validations

def check_load_conditions(df, last_import_date):
    """
    Sprawdzenie warunku wykonania importu plików do bazy.
    Sprawdzamy, czy czas ostatniego importu pobrany z pliku params
    jest wcześniejszy niż data danych z API
    """
    date, hour = df.loc[0, "data_pomiaru"], df.loc[0, "godzina_pomiaru"]
    df_datetime = convert_datetime(date, hour)
    if last_import_date < df_datetime:
        return df_datetime
