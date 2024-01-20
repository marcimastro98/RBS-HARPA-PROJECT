CREATE SCHEMA IF NOT EXISTS HARPA;



-- Creazione delle tabelle con una colonna timestamp anziché date per avere anche l'orario
CREATE TABLE HARPA.edificio (
    id SERIAL PRIMARY KEY,
    data TIMESTAMP NOT NULL,
    kilowatt NUMERIC(10, 2)
);

CREATE TABLE HARPA.data_center (
    id SERIAL PRIMARY KEY,
    data TIMESTAMP NOT NULL,
    kilowatt NUMERIC(10, 2)
);

CREATE TABLE HARPA.fotovoltaico (
    id SERIAL PRIMARY KEY,
    data TIMESTAMP NOT NULL,
    kilowatt NUMERIC(10, 2)
);


COPY HARPA.data_center(data, kilowatt)
FROM '/csv/Generale_Data_Center_Energia_Attiva.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',');

COPY HARPA.edificio(data, kilowatt)
FROM '/csv/Generale_Edificio_Energia_Attiva.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',');

COPY HARPA.fotovoltaico(data, kilowatt)
FROM '/csv/Impianto_Fotovoltaico_Energia_Attiva_Prodotta.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',');


CREATE TABLE HARPA.aggregazione_ora AS
SELECT
  DATE_TRUNC('hour', data) AS ora,
  TO_CHAR(DATE_TRUNC('hour', data), 'Day') AS giorno_settimana,
  MAX(CASE WHEN source = 'data_center' THEN kilowatt ELSE NULL END) -
  MIN(CASE WHEN source = 'data_center' THEN kilowatt ELSE NULL END) AS kilowatt_data_center_diff,
  MAX(CASE WHEN source = 'edificio' THEN kilowatt ELSE NULL END) -
  MIN(CASE WHEN source = 'edificio' THEN kilowatt ELSE NULL END) AS kilowatt_edificio_diff,
  MAX(CASE WHEN source = 'fotovoltaico' THEN kilowatt ELSE NULL END) -
  MIN(CASE WHEN source = 'fotovoltaico' THEN kilowatt ELSE NULL END) AS kilowatt_fotovoltaico_diff,
  (MAX(CASE WHEN source = 'edificio' THEN kilowatt ELSE NULL END) -
  MIN(CASE WHEN source = 'edificio' THEN kilowatt ELSE NULL END)) -
  (MAX(CASE WHEN source = 'data_center' THEN kilowatt ELSE NULL END) -
  MIN(CASE WHEN source = 'data_center' THEN kilowatt ELSE NULL END)) +
  (MAX(CASE WHEN source = 'fotovoltaico' THEN kilowatt ELSE NULL END) -
  MIN(CASE WHEN source = 'fotovoltaico' THEN kilowatt ELSE NULL END)) AS kilowatt_ufficio_diff
FROM (
  SELECT data, kilowatt, 'data_center' AS source FROM HARPA.data_center
  UNION ALL
  SELECT data, kilowatt, 'edificio' AS source FROM HARPA.edificio
  UNION ALL
  SELECT data, kilowatt, 'fotovoltaico' AS source FROM HARPA.fotovoltaico
) AS sub
GROUP BY ora, TO_CHAR(DATE_TRUNC('hour', sub.data), 'Day');


CREATE TABLE HARPA.aggregazione_fascia_oraria AS
SELECT
  DATE(data) AS giorno,
  TO_CHAR(data, 'Day') AS giorno_settimana,
  CASE
    WHEN EXTRACT(HOUR FROM data) >= 0 AND EXTRACT(HOUR FROM data) < 9 THEN '00:00-09:00'
    WHEN EXTRACT(HOUR FROM data) >= 9 AND EXTRACT(HOUR FROM data) < 18 THEN '09:00-18:00'
    WHEN EXTRACT(HOUR FROM data) >= 18 THEN '18:00-00:00'
  END AS fascia_oraria,
  MAX(CASE WHEN source = 'data_center' THEN kilowatt END) -
  MIN(CASE WHEN source = 'data_center' THEN kilowatt END) AS kilowatt_data_center_diff,
  MAX(CASE WHEN source = 'edificio' THEN kilowatt END) -
  MIN(CASE WHEN source = 'edificio' THEN kilowatt END) AS kilowatt_edificio_diff,
  MAX(CASE WHEN source = 'fotovoltaico' THEN kilowatt END) -
  MIN(CASE WHEN source = 'fotovoltaico' THEN kilowatt END) AS kilowatt_fotovoltaico_diff,
  (MAX(CASE WHEN source = 'edificio' THEN kilowatt END) -
  MIN(CASE WHEN source = 'edificio' THEN kilowatt END)) -
  (MAX(CASE WHEN source = 'data_center' THEN kilowatt END) -
  MIN(CASE WHEN source = 'data_center' THEN kilowatt END)) +
  (MAX(CASE WHEN source = 'fotovoltaico' THEN kilowatt END) -
  MIN(CASE WHEN source = 'fotovoltaico' THEN kilowatt END)) AS kilowatt_ufficio_diff
FROM (
  SELECT data, kilowatt, 'data_center' AS source FROM HARPA.data_center
  UNION ALL
  SELECT data, kilowatt, 'edificio' AS source FROM HARPA.edificio
  UNION ALL
  SELECT data, kilowatt, 'fotovoltaico' AS source FROM HARPA.fotovoltaico
) AS sub
GROUP BY giorno, TO_CHAR(data, 'Day'), fascia_oraria;




CREATE TABLE HARPA.aggregazione_giorno AS
SELECT
  DATE_TRUNC('day', data)::date AS giorno,
  TO_CHAR(data, 'Day') AS giorno_settimana,
  MAX(CASE WHEN source = 'data_center' THEN kilowatt ELSE NULL END) -
  MIN(CASE WHEN source = 'data_center' THEN kilowatt ELSE NULL END) AS kilowatt_data_center_diff,
  MAX(CASE WHEN source = 'edificio' THEN kilowatt ELSE NULL END) -
  MIN(CASE WHEN source = 'edificio' THEN kilowatt ELSE NULL END) AS kilowatt_edificio_diff,
  MAX(CASE WHEN source = 'fotovoltaico' THEN kilowatt ELSE NULL END) -
  MIN(CASE WHEN source = 'fotovoltaico' THEN kilowatt ELSE NULL END) AS kilowatt_fotovoltaico_diff,
  (MAX(CASE WHEN source = 'edificio' THEN kilowatt ELSE NULL END) -
  MIN(CASE WHEN source = 'edificio' THEN kilowatt ELSE NULL END)) -
  (MAX(CASE WHEN source = 'data_center' THEN kilowatt ELSE NULL END) -
  MIN(CASE WHEN source = 'data_center' THEN kilowatt ELSE NULL END)) +
  (MAX(CASE WHEN source = 'fotovoltaico' THEN kilowatt ELSE NULL END) -
  MIN(CASE WHEN source = 'fotovoltaico' THEN kilowatt ELSE NULL END)) AS kilowatt_ufficio_diff
FROM (
  SELECT data, kilowatt, 'data_center' AS source FROM HARPA.data_center
  UNION ALL
  SELECT data, kilowatt, 'edificio' AS source FROM HARPA.edificio
  UNION ALL
  SELECT data, kilowatt, 'fotovoltaico' AS source FROM HARPA.fotovoltaico
) AS sub
GROUP BY DATE_TRUNC('day', data)::date, TO_CHAR(data, 'Day');

CREATE TABLE HARPA.aggregazione_mese AS
SELECT
TO_CHAR(data, 'YYYY-MM') AS anno_mese,
  MAX(CASE WHEN source = 'data_center' THEN kilowatt ELSE NULL END) -
  MIN(CASE WHEN source = 'data_center' THEN kilowatt ELSE NULL END) AS kilowatt_data_center_diff,
  MAX(CASE WHEN source = 'edificio' THEN kilowatt ELSE NULL END) -
  MIN(CASE WHEN source = 'edificio' THEN kilowatt ELSE NULL END) AS kilowatt_edificio_diff,
  MAX(CASE WHEN source = 'fotovoltaico' THEN kilowatt ELSE NULL END) -
  MIN(CASE WHEN source = 'fotovoltaico' THEN kilowatt ELSE NULL END) AS kilowatt_fotovoltaico_diff,
  (MAX(CASE WHEN source = 'edificio' THEN kilowatt ELSE NULL END) -
  MIN(CASE WHEN source = 'edificio' THEN kilowatt ELSE NULL END)) -
  (MAX(CASE WHEN source = 'data_center' THEN kilowatt ELSE NULL END) -
  MIN(CASE WHEN source = 'data_center' THEN kilowatt ELSE NULL END)) +
  (MAX(CASE WHEN source = 'fotovoltaico' THEN kilowatt ELSE NULL END) -
  MIN(CASE WHEN source = 'fotovoltaico' THEN kilowatt ELSE NULL END)) AS kilowatt_ufficio_diff
FROM (
  SELECT data, kilowatt, 'data_center' AS source FROM HARPA.data_center
  UNION ALL
  SELECT data, kilowatt, 'edificio' AS source FROM HARPA.edificio
  UNION ALL
  SELECT data, kilowatt, 'fotovoltaico' AS source FROM HARPA.fotovoltaico
) AS sub
GROUP BY TO_CHAR(data, 'YYYY-MM');

CREATE TABLE HARPA.aggregazione_anno AS
SELECT
  EXTRACT(YEAR FROM data) AS anno,
  MAX(CASE WHEN source = 'data_center' THEN kilowatt ELSE NULL END) -
  MIN(CASE WHEN source = 'data_center' THEN kilowatt ELSE NULL END) AS kilowatt_data_center_diff,
  MAX(CASE WHEN source = 'edificio' THEN kilowatt ELSE NULL END) -
  MIN(CASE WHEN source = 'edificio' THEN kilowatt ELSE NULL END) AS kilowatt_edificio_diff,
  MAX(CASE WHEN source = 'fotovoltaico' THEN kilowatt ELSE NULL END) -
  MIN(CASE WHEN source = 'fotovoltaico' THEN kilowatt ELSE NULL END) AS kilowatt_fotovoltaico_diff,
  (MAX(CASE WHEN source = 'edificio' THEN kilowatt ELSE NULL END) -
  MIN(CASE WHEN source = 'edificio' THEN kilowatt ELSE NULL END)) -
  (MAX(CASE WHEN source = 'data_center' THEN kilowatt ELSE NULL END) -
  MIN(CASE WHEN source = 'data_center' THEN kilowatt ELSE NULL END)) +
  (MAX(CASE WHEN source = 'fotovoltaico' THEN kilowatt ELSE NULL END) -
  MIN(CASE WHEN source = 'fotovoltaico' THEN kilowatt ELSE NULL END)) AS kilowatt_ufficio_diff
FROM (
  SELECT data, kilowatt, 'data_center' AS source FROM HARPA.data_center
  UNION ALL
  SELECT data, kilowatt, 'edificio' AS source FROM HARPA.edificio
  UNION ALL
  SELECT data, kilowatt, 'fotovoltaico' AS source FROM HARPA.fotovoltaico
) AS sub
GROUP BY EXTRACT(YEAR FROM data);
