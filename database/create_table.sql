USE [ZadanieETL];

IF OBJECT_ID('ZadanieETL.dbo.POGODA_W_POLSCE') IS NULL
CREATE TABLE [dbo].[POGODA_W_POLSCE](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[id_stacji] [int] NULL,
	[stacja] [varchar](100) NULL,
	[data_pomiaru] [date] NULL,
	[godzina_pomiaru] [tinyint] NULL,
	[temperatura] [decimal](4, 1) NULL,
	[predkosc_wiatru] [smallint] NULL,
	[kierunek_wiatru] [smallint] NULL,
	[wilgotnosc_wzgledna] [decimal](4, 1) NULL,
	[suma_opadu] [decimal](5, 1) NULL,
	[cisnienie] [decimal](6, 1) NULL,
	[roznica_od_wzorcowego] [decimal](6, 1) NULL,
);