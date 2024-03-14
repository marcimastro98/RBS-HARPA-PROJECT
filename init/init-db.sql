CREATE TABLE IF NOT EXISTS HARPA.data_center (
    id SERIAL PRIMARY KEY,
    data TIMESTAMP NOT NULL UNIQUE,
    kilowatt NUMERIC(10, 2)
);
INSERT INTO HARPA.data_center(data, kilowatt)
SELECT data, kilowatt FROM HARPA.temp_data_center;


CREATE TABLE IF NOT EXISTS HARPA.edificio (
    id SERIAL PRIMARY KEY,
    data TIMESTAMP NOT NULL UNIQUE,
    kilowatt NUMERIC(10, 2)
);
INSERT INTO HARPA.edificio(data, kilowatt)
SELECT data, kilowatt FROM HARPA.temp_edificio;


CREATE TABLE IF NOT EXISTS HARPA.fotovoltaico (
    id SERIAL PRIMARY KEY,
    data TIMESTAMP NOT NULL UNIQUE,
    kilowatt NUMERIC(10, 2)
);
INSERT INTO HARPA.fotovoltaico(data, kilowatt)
SELECT data, kilowatt FROM HARPA.temp_fotovoltaico;



CREATE TABLE IF NOT EXISTS HARPA.ufficio (
    id SERIAL PRIMARY KEY,
    data TIMESTAMP NOT NULL UNIQUE,
    kilowatt NUMERIC(10, 2)
);


INSERT INTO HARPA.ufficio (data, kilowatt)
SELECT data, SUM(kilowatt) AS kilowatt
FROM (
    SELECT DISTINCT COALESCE(e.data, dc.data, f.data) AS data,
    (COALESCE(e.kilowatt, 0) - COALESCE(dc.kilowatt, 0) + COALESCE(f.kilowatt, 0)) AS kilowatt
    FROM HARPA.edificio e
    FULL OUTER JOIN HARPA.data_center dc ON e.data = dc.data
    FULL OUTER JOIN HARPA.fotovoltaico f ON COALESCE(e.data, dc.data) = f.data
) AS combined
GROUP BY data
ON CONFLICT (data) DO NOTHING;



CREATE TABLE IF NOT EXISTS HARPA.unione_dataset (
    id SERIAL PRIMARY KEY,
    data TIMESTAMP NOT NULL UNIQUE,
    kilowatt_edificio NUMERIC(10, 2),
    kilowatt_data_center NUMERIC(10, 2),
    kilowatt_fotovoltaico NUMERIC(10, 2),
    kilowatt_ufficio NUMERIC(10, 2),
    fascia_oraria INTEGER,
    giorno_settimana INTEGER
);


INSERT INTO HARPA.unione_dataset (data, kilowatt_edificio, kilowatt_data_center, kilowatt_fotovoltaico, kilowatt_ufficio, fascia_oraria, giorno_settimana)
SELECT
    COALESCE(e.data, dc.data, f.data) AS data,
    COALESCE(e.kilowatt, 0) AS kilowatt_edificio,
    COALESCE(dc.kilowatt, 0) AS kilowatt_data_center,
    COALESCE(f.kilowatt, 0) AS kilowatt_fotovoltaico,
    COALESCE(e.kilowatt, 0) - COALESCE(dc.kilowatt, 0) + COALESCE(f.kilowatt, 0) AS kilowatt_ufficio,
    CASE
        WHEN EXTRACT(HOUR FROM COALESCE(e.data, dc.data, f.data)) < 9 THEN 1
        WHEN EXTRACT(HOUR FROM COALESCE(e.data, dc.data, f.data)) >= 9 AND EXTRACT(HOUR FROM COALESCE(e.data, dc.data, f.data)) < 19 THEN 2
        WHEN EXTRACT(HOUR FROM COALESCE(e.data, dc.data, f.data)) >= 19 THEN 3
    END AS fascia_oraria,
    EXTRACT(DOW FROM COALESCE(e.data, dc.data, f.data)) AS giorno_settimana
FROM
    HARPA.edificio e
FULL OUTER JOIN HARPA.data_center dc ON e.data = dc.data
FULL OUTER JOIN HARPA.fotovoltaico f ON e.data = f.data
WHERE e.data IS NOT NULL AND dc.data IS NOT NULL
ORDER BY data
ON CONFLICT (data) DO NOTHING;


CREATE TABLE IF NOT EXISTS HARPA.aggregazione_ora (
    id SERIAL PRIMARY KEY,
    data TIMESTAMP NOT NULL UNIQUE,
    fascia_oraria INTEGER,
    kilowatt_edificio NUMERIC(10, 2),
    kilowatt_data_center NUMERIC(10, 2),
    kilowatt_fotovoltaico NUMERIC(10, 2),
    kilowatt_ufficio NUMERIC(10, 2),
    giorno_settimana INTEGER,
    rain NUMERIC(10, 2),
    cloud_cover NUMERIC(10, 2),
    relative_humidity_2m NUMERIC(10, 2),
    wind_speed_10m NUMERIC(10, 2),
    wind_direction_10m NUMERIC(10, 2),
    temperature_2m NUMERIC(10, 2),
    dew_point_2m NUMERIC(10, 2),
    apparent_temperature NUMERIC(10, 2),
    precipitation NUMERIC(10, 2),
    snowfall NUMERIC(10, 2),
    snow_depth NUMERIC(10, 2),
    weather_code NUMERIC(10, 2),
    pressure_msl NUMERIC(10, 2),
    surface_pressure NUMERIC(10, 2),
    cloud_cover_low NUMERIC(10, 2),
    cloud_cover_mid NUMERIC(10, 2),
    cloud_cover_high NUMERIC(10, 2)
);

CREATE TABLE IF NOT EXISTS HARPA.aggregazione_giorno (
    id SERIAL PRIMARY KEY,
    data TIMESTAMP NOT NULL UNIQUE,
    kilowatt_edificio NUMERIC(10, 2),
    kilowatt_data_center NUMERIC(10, 2),
    kilowatt_fotovoltaico NUMERIC(10, 2),
    kilowatt_ufficio NUMERIC(10, 2),
    giorno_settimana INTEGER,
    rain NUMERIC(10, 2),
    cloud_cover NUMERIC(10, 2),
    relative_humidity_2m NUMERIC(10, 2),
    wind_speed_10m NUMERIC(10, 2),
    wind_direction_10m NUMERIC(10, 2),
    temperature_2m NUMERIC(10, 2),
    dew_point_2m NUMERIC(10, 2),
    apparent_temperature NUMERIC(10, 2),
    precipitation NUMERIC(10, 2),
    snowfall NUMERIC(10, 2),
    snow_depth NUMERIC(10, 2),
    weather_code NUMERIC(10, 2),
    pressure_msl NUMERIC(10, 2),
    surface_pressure NUMERIC(10, 2),
    cloud_cover_low NUMERIC(10, 2),
    cloud_cover_mid NUMERIC(10, 2),
    cloud_cover_high NUMERIC(10, 2)
);

CREATE TABLE IF NOT EXISTS HARPA.aggregazione_mese (
    id SERIAL PRIMARY KEY,
    data TIMESTAMP NOT NULL UNIQUE,
    kilowatt_edificio NUMERIC(10, 2),
    kilowatt_data_center NUMERIC(10, 2),
    kilowatt_fotovoltaico NUMERIC(10, 2),
    kilowatt_ufficio NUMERIC(10, 2),
    rain NUMERIC(10, 2),
    cloud_cover NUMERIC(10, 2),
    relative_humidity_2m NUMERIC(10, 2),
    wind_speed_10m NUMERIC(10, 2),
    wind_direction_10m NUMERIC(10, 2),
    temperature_2m NUMERIC(10, 2),
    dew_point_2m NUMERIC(10, 2),
    apparent_temperature NUMERIC(10, 2),
    precipitation NUMERIC(10, 2),
    snowfall NUMERIC(10, 2),
    snow_depth NUMERIC(10, 2),
    weather_code NUMERIC(10, 2),
    pressure_msl NUMERIC(10, 2),
    surface_pressure NUMERIC(10, 2),
    cloud_cover_low NUMERIC(10, 2),
    cloud_cover_mid NUMERIC(10, 2),
    cloud_cover_high NUMERIC(10, 2)
);

CREATE TABLE IF NOT EXISTS HARPA.aggregazione_anno (
    id SERIAL PRIMARY KEY,
    data TIMESTAMP NOT NULL UNIQUE,
    kilowatt_edificio NUMERIC(10, 2),
    kilowatt_data_center NUMERIC(10, 2),
    kilowatt_fotovoltaico NUMERIC(10, 2),
    kilowatt_ufficio NUMERIC(10, 2),
    rain NUMERIC(10, 2),
    cloud_cover NUMERIC(10, 2),
    relative_humidity_2m NUMERIC(10, 2),
    wind_speed_10m NUMERIC(10, 2),
    wind_direction_10m NUMERIC(10, 2),
    temperature_2m NUMERIC(10, 2),
    dew_point_2m NUMERIC(10, 2),
    apparent_temperature NUMERIC(10, 2),
    precipitation NUMERIC(10, 2),
    snowfall NUMERIC(10, 2),
    snow_depth NUMERIC(10, 2),
    weather_code NUMERIC(10, 2),
    pressure_msl NUMERIC(10, 2),
    surface_pressure NUMERIC(10, 2),
    cloud_cover_low NUMERIC(10, 2),
    cloud_cover_mid NUMERIC(10, 2),
    cloud_cover_high NUMERIC(10, 2)
);

CREATE TABLE IF NOT EXISTS HARPA.aggregazione_fascia_oraria (
    id SERIAL PRIMARY KEY,
    data DATE NOT NULL,
    fascia_oraria INTEGER,
    kilowatt_edificio NUMERIC(10, 2),
    kilowatt_data_center NUMERIC(10, 2),
    kilowatt_fotovoltaico NUMERIC(10, 2),
    kilowatt_ufficio NUMERIC(10, 2),
    giorno_settimana INTEGER,
    rain NUMERIC(10, 2),
    cloud_cover NUMERIC(10, 2),
    relative_humidity_2m NUMERIC(10, 2),
    wind_speed_10m NUMERIC(10, 2),
    wind_direction_10m NUMERIC(10, 2),
    temperature_2m NUMERIC(10, 2),
    dew_point_2m NUMERIC(10, 2),
    apparent_temperature NUMERIC(10, 2),
    precipitation NUMERIC(10, 2),
    snowfall NUMERIC(10, 2),
    snow_depth NUMERIC(10, 2),
    weather_code NUMERIC(10, 2),
    pressure_msl NUMERIC(10, 2),
    surface_pressure NUMERIC(10, 2),
    cloud_cover_low NUMERIC(10, 2),
    cloud_cover_mid NUMERIC(10, 2),
    cloud_cover_high NUMERIC(10, 2),
    UNIQUE (data, fascia_oraria)
);


INSERT INTO HARPA.aggregazione_ora (
    data,
    kilowatt_edificio,
    kilowatt_data_center,
    kilowatt_fotovoltaico,
    kilowatt_ufficio,
    giorno_settimana
)
SELECT DISTINCT
    DATE_TRUNC('hour', data) AS data,
    LAST_VALUE(kilowatt_edificio) OVER w - FIRST_VALUE(kilowatt_edificio) OVER w AS differenza_kilowatt_edificio,
    LAST_VALUE(kilowatt_data_center) OVER w - FIRST_VALUE(kilowatt_data_center) OVER w AS differenza_kilowatt_data_center,
    LAST_VALUE(kilowatt_fotovoltaico) OVER w - FIRST_VALUE(kilowatt_fotovoltaico) OVER w AS differenza_kilowatt_fotovoltaico,
    LAST_VALUE(kilowatt_ufficio) OVER w - FIRST_VALUE(kilowatt_ufficio) OVER w AS differenza_kilowatt_ufficio,
    EXTRACT(DOW FROM data) AS giorno_settimana
FROM HARPA.unione_dataset
WINDOW w AS (
    PARTITION BY DATE_TRUNC('hour', data), EXTRACT(DOW FROM data)
    ORDER BY data
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
)
ORDER BY data
ON CONFLICT (data) DO NOTHING;


INSERT INTO HARPA.aggregazione_giorno (
    data,
    kilowatt_edificio,
    kilowatt_data_center,
    kilowatt_fotovoltaico,
    kilowatt_ufficio,
    giorno_settimana
)
SELECT DISTINCT
    DATE_TRUNC('day', data) AS data,
    LAST_VALUE(kilowatt_edificio) OVER w - FIRST_VALUE(kilowatt_edificio) OVER w AS differenza_kilowatt_edificio,
    LAST_VALUE(kilowatt_data_center) OVER w - FIRST_VALUE(kilowatt_data_center) OVER w AS differenza_kilowatt_data_center,
    LAST_VALUE(kilowatt_fotovoltaico) OVER w - FIRST_VALUE(kilowatt_fotovoltaico) OVER w AS differenza_kilowatt_fotovoltaico,
    LAST_VALUE(kilowatt_ufficio) OVER w - FIRST_VALUE(kilowatt_ufficio) OVER w AS differenza_kilowatt_ufficio,
    EXTRACT(DOW FROM data) AS giorno_settimana
FROM HARPA.unione_dataset
WINDOW w AS (
    PARTITION BY DATE_TRUNC('day', data), EXTRACT(DOW FROM data)
    ORDER BY data
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
)
ORDER BY data
ON CONFLICT (data) DO NOTHING;


INSERT INTO HARPA.aggregazione_mese (
    data,
    kilowatt_edificio,
    kilowatt_data_center,
    kilowatt_fotovoltaico,
    kilowatt_ufficio
)
SELECT
    primo_giorno_mese AS data,
    (ultimo_valore.kilowatt_edificio - primo_valore.kilowatt_edificio) AS kilowatt_edificio,
    (ultimo_valore.kilowatt_data_center - primo_valore.kilowatt_data_center) AS kilowatt_data_center,
    (ultimo_valore.kilowatt_fotovoltaico - primo_valore.kilowatt_fotovoltaico) AS kilowatt_fotovoltaico,
    (ultimo_valore.kilowatt_ufficio - primo_valore.kilowatt_ufficio) AS kilowatt_ufficio
FROM
    (SELECT DATE_TRUNC('month', data) AS primo_giorno_mese FROM HARPA.unione_dataset GROUP BY primo_giorno_mese) AS mesi
JOIN
    (SELECT data, kilowatt_edificio, kilowatt_data_center, kilowatt_fotovoltaico, kilowatt_ufficio FROM HARPA.unione_dataset WHERE data IN (SELECT MIN(data) FROM HARPA.unione_dataset GROUP BY DATE_TRUNC('month', data))) AS primo_valore
ON mesi.primo_giorno_mese = DATE_TRUNC('month', primo_valore.data)
JOIN
    (SELECT data, kilowatt_edificio, kilowatt_data_center, kilowatt_fotovoltaico, kilowatt_ufficio FROM HARPA.unione_dataset WHERE data IN (SELECT MAX(data) FROM HARPA.unione_dataset GROUP BY DATE_TRUNC('month', data))) AS ultimo_valore
ON mesi.primo_giorno_mese = DATE_TRUNC('month', ultimo_valore.data)
ORDER BY primo_giorno_mese
ON CONFLICT (data) DO NOTHING;



INSERT INTO HARPA.aggregazione_anno (
    data,
    kilowatt_edificio,
    kilowatt_data_center,
    kilowatt_fotovoltaico,
    kilowatt_ufficio
)
SELECT
    primo_giorno_anno AS data,
    (ultimo_valore.kilowatt_edificio - primo_valore.kilowatt_edificio) AS kilowatt_edificio,
    (ultimo_valore.kilowatt_data_center - primo_valore.kilowatt_data_center) AS kilowatt_data_center,
    (ultimo_valore.kilowatt_fotovoltaico - primo_valore.kilowatt_fotovoltaico) AS kilowatt_fotovoltaico,
    (ultimo_valore.kilowatt_ufficio - primo_valore.kilowatt_ufficio) AS kilowatt_ufficio
FROM
    (SELECT DATE_TRUNC('year', data) AS primo_giorno_anno FROM HARPA.unione_dataset GROUP BY primo_giorno_anno) AS anni
JOIN
    (SELECT data, kilowatt_edificio, kilowatt_data_center, kilowatt_fotovoltaico, kilowatt_ufficio FROM HARPA.unione_dataset WHERE data IN (SELECT MIN(data) FROM HARPA.unione_dataset GROUP BY DATE_TRUNC('year', data))) AS primo_valore
ON anni.primo_giorno_anno = DATE_TRUNC('year', primo_valore.data)
JOIN
    (SELECT data, kilowatt_edificio, kilowatt_data_center, kilowatt_fotovoltaico, kilowatt_ufficio FROM HARPA.unione_dataset WHERE data IN (SELECT MAX(data) FROM HARPA.unione_dataset GROUP BY DATE_TRUNC('year', data))) AS ultimo_valore
ON anni.primo_giorno_anno = DATE_TRUNC('year', ultimo_valore.data)
ORDER BY primo_giorno_anno
ON CONFLICT (data) DO NOTHING;


INSERT INTO HARPA.aggregazione_fascia_oraria (
    data,
    kilowatt_edificio,
    kilowatt_data_center,
    kilowatt_fotovoltaico,
    kilowatt_ufficio,
    fascia_oraria,
    giorno_settimana
)
SELECT
    DATE(giorni.primo) AS data,
    (ultimo_valore.kilowatt_edificio - primo_valore.kilowatt_edificio) AS kilowatt_edificio,
    (ultimo_valore.kilowatt_data_center - primo_valore.kilowatt_data_center) AS kilowatt_data_center,
    (ultimo_valore.kilowatt_fotovoltaico - primo_valore.kilowatt_fotovoltaico) AS kilowatt_fotovoltaico,
    (ultimo_valore.kilowatt_ufficio - primo_valore.kilowatt_ufficio) AS kilowatt_ufficio,
    giorni.fascia_oraria,
    giorni.giorno_settimana
FROM
    (SELECT DATE(data) AS primo, fascia_oraria, giorno_settimana
     FROM HARPA.unione_dataset
     GROUP BY DATE(data), fascia_oraria, giorno_settimana) AS giorni
JOIN
    (SELECT data, kilowatt_edificio, kilowatt_data_center, kilowatt_fotovoltaico, kilowatt_ufficio, fascia_oraria, giorno_settimana
     FROM HARPA.unione_dataset
     WHERE (data, fascia_oraria, giorno_settimana) IN
         (SELECT MIN(data), fascia_oraria, giorno_settimana
          FROM HARPA.unione_dataset
          GROUP BY DATE(data), fascia_oraria, giorno_settimana)) AS primo_valore
ON DATE(giorni.primo) = DATE(primo_valore.data)
AND giorni.fascia_oraria = primo_valore.fascia_oraria
AND giorni.giorno_settimana = primo_valore.giorno_settimana
JOIN
    (SELECT data, kilowatt_edificio, kilowatt_data_center, kilowatt_fotovoltaico, kilowatt_ufficio, fascia_oraria, giorno_settimana
     FROM HARPA.unione_dataset
     WHERE (data, fascia_oraria, giorno_settimana) IN
         (SELECT MAX(data), fascia_oraria, giorno_settimana
          FROM HARPA.unione_dataset
          GROUP BY DATE(data), fascia_oraria, giorno_settimana)) AS ultimo_valore
ON DATE(giorni.primo) = DATE(ultimo_valore.data)
AND giorni.fascia_oraria = ultimo_valore.fascia_oraria
AND giorni.giorno_settimana = ultimo_valore.giorno_settimana
ORDER BY DATE(giorni.primo), giorni.fascia_oraria, giorni.giorno_settimana
ON CONFLICT (data, fascia_oraria) DO NOTHING;


DELETE FROM harpa.aggregazione_ora
WHERE
(kilowatt_ufficio IS NULL
OR kilowatt_edificio IS NULL
OR kilowatt_data_center IS NULL)
OR
(kilowatt_ufficio = 0
AND kilowatt_edificio = 0
AND kilowatt_data_center = 0);

DELETE FROM harpa.aggregazione_giorno
WHERE
(kilowatt_ufficio IS NULL
OR kilowatt_edificio IS NULL
OR kilowatt_data_center IS NULL)
OR
(kilowatt_ufficio = 0
AND kilowatt_edificio = 0
AND kilowatt_data_center = 0);

DELETE FROM harpa.aggregazione_mese
WHERE
(kilowatt_ufficio IS NULL
OR kilowatt_edificio IS NULL
OR kilowatt_data_center IS NULL)
OR
(kilowatt_ufficio = 0
AND kilowatt_edificio = 0
AND kilowatt_data_center = 0);

DELETE FROM harpa.aggregazione_anno
WHERE
(kilowatt_ufficio IS NULL
OR kilowatt_edificio IS NULL
OR kilowatt_data_center IS NULL)
OR
(kilowatt_ufficio = 0
AND kilowatt_edificio = 0
AND kilowatt_data_center = 0);

DELETE FROM harpa.aggregazione_fascia_oraria
WHERE
(kilowatt_ufficio IS NULL
OR kilowatt_edificio IS NULL
OR kilowatt_data_center IS NULL)
OR
(kilowatt_ufficio = 0
AND kilowatt_edificio = 0
AND kilowatt_data_center = 0);

DROP TABLE IF EXISTS HARPA.temp_data_center;
DROP TABLE IF EXISTS HARPA.temp_edificio;
DROP TABLE IF EXISTS HARPA.temp_fotovoltaico;