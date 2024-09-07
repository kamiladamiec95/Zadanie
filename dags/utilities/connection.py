from airflow.models import Connection
from airflow.utils.db import provide_session
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine


@provide_session
def create_conn_airflow(session=None):
    """
    Funkcja tworząca połączenie mssql w Airflow, jeśli jeszcze nie istnieje.
    """
    conn_id = 'localhost_mssql'
    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()

    if existing_conn is None:
        conn = Connection(
            conn_id=conn_id,
            conn_type='mssql',  # The type of the connection
            host='host.docker.internal',  # The SQL Server host
            login='airflow',  # Username for SQL Server
            password='airflow',  # Password for SQL Server
            schema='ZadanieETL',  # Default database
            port=1433  # SQL Server port
        )
        session.add(conn)
        session.commit()


def create_conn_engine(conn_id):
    """Funkcja tworząca połączenie do sql. Pobiera dane zapisane w connection w Airflow
     na podstawie podanego conn_id i na tej podstawie tworzy połączenie
    """
    conn = BaseHook.get_connection(conn_id)
    if conn.conn_type == "mssql":
        connection_string = (
            f"mssql+pyodbc://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
            f"?driver=ODBC+Driver+17+for+SQL+Server")
        engine = create_engine(connection_string)
    return engine


# from airflow.models import Connection
# from airflow.utils.db import provide_session
# from airflow.hooks.base import BaseHook
# from sqlalchemy import create_engine
# import logging
#
# # Configure logger
# logger = logging.getLogger(__name__)
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
#
#
# @provide_session
# def create_conn_airflow(session=None):
#     """
#     Creates an MSSQL connection in Airflow if it does not already exist.
#     This function checks if a connection with the conn_id 'localhost_mssql' exists in the Airflow metadata database.
#     If it doesn't, it creates one with the necessary parameters like host, login, password, schema, and port.
#
#     :param session: Airflow session object, injected by the @provide_session decorator.
#     """
#     conn_id = 'localhost_mssql'
#     logger.info(f"Checking if connection '{conn_id}' exists in Airflow.")
#
#     try:
#         # Check if the connection already exists in Airflow
#         existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
#
#         if existing_conn is None:
#             logger.info(f"Connection '{conn_id}' does not exist. Creating a new connection.")
#             # Create the new MSSQL connection
#             conn = Connection(
#                 conn_id=conn_id,
#                 conn_type='mssql',  # The type of the connection (MSSQL in this case)
#                 host='host.docker.internal',  # SQL Server host (in Docker environment)
#                 login='airflow',  # Username for the SQL Server
#                 password='airflow',  # Password for the SQL Server
#                 schema='ZadanieETL',  # Default database schema
#                 port=1433  # MSSQL server port
#             )
#             # Add and commit the connection to the session
#             session.add(conn)
#             session.commit()
#             logger.info(f"Successfully created the MSSQL connection '{conn_id}'.")
#         else:
#             logger.info(f"Connection '{conn_id}' already exists. Skipping creation.")
#
#     except Exception as e:
#         logger.error(f"Failed to create or check connection '{conn_id}': {e}")
#         session.rollback()  # Rollback the session in case of failure
#         raise
#
#
# def create_conn_engine(conn_id):
#     """
#     Creates a connection engine to an MSSQL database using connection details stored in Airflow.
#     This function fetches the connection details (login, password, host, port, schema) from Airflow's
#     stored connections using the provided conn_id and creates a SQLAlchemy engine.
#
#     :param conn_id: The Airflow connection ID to use for creating the engine.
#     :return: SQLAlchemy engine object for interacting with the MSSQL database.
#     """
#     logger.info(f"Creating connection engine for connection ID: {conn_id}")
#
#     try:
#         # Fetch connection details from Airflow using BaseHook
#         conn = BaseHook.get_connection(conn_id)
#         logger.info(f"Fetched connection details for conn_id: {conn_id}")
#
#         # Check if the connection type is MSSQL and form the appropriate connection string
#         if conn.conn_type == "mssql":
#             connection_string = (
#                 f"mssql+pyodbc://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
#                 f"?driver=ODBC+Driver+17+for+SQL+Server"
#             )
#             # Create the SQLAlchemy engine using the connection string
#             engine = create_engine(connection_string)
#             logger.info(f"Successfully created engine for connection ID: {conn_id}")
#             return engine
#         else:
#             raise ValueError(f"Unsupported connection type '{conn.conn_type}' for conn_id: {conn_id}")
#
#     except Exception as e:
#         logger.error(f"Error creating engine for connection ID '{conn_id}': {e}")
#         raise
