import json
from airflow.utils.log.logging_mixin import LoggingMixin

PARAMS_FILE_PATH = "/opt/airflow/dags/utilities/params.json"


logger = LoggingMixin().log

def get_param_values(param_names: list) -> tuple:
    params = []
    for param_name in param_names:
        with open(PARAMS_FILE_PATH, "r") as f:
            file_data = json.load(f)
        param = file_data[param_name]
        params.append(param)
    return tuple(params)


def change_last_import_date(new_date: str):
    """
    Funkcja odpowiedzialna za zmianę daty ostatniego importu w pliku z parametrami
    Ścieżka do pliku w kontenerze Airflow
    """
    try:
        with open(PARAMS_FILE_PATH, "r") as f:
            file_data = json.load(f)
        file_data['last_import_date'] = new_date
        with open(PARAMS_FILE_PATH, "w") as f:
            json.dump(file_data, f)
        logger.info(f"Last import date successfully updated to {new_date}.")
    except Exception as e:
        logger.error(f"Error updating last import date: {e}")
        raise







