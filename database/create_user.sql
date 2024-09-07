USE [ZadanieETL];

IF SUSER_ID('airflow') IS NULL
BEGIN
	CREATE LOGIN airflow WITH PASSWORD = 'airflow';

	CREATE USER airflow FOR LOGIN airflow;

	ALTER ROLE db_datawriter ADD MEMBER airflow;
	ALTER ROLE db_datareader ADD MEMBER airflow;
END