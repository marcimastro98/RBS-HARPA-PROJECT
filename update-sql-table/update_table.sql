-- Assicurati di avere i permessi necessari per eseguire queste operazioni e che il database sia configurato per accettare connessioni locali per COPY.

-- Passo 1: Creazione delle tabelle di staging
CREATE TEMP TABLE IF NOT EXISTS HARPA.edificio_staging (LIKE HARPA.edificio INCLUDING ALL);
CREATE TEMP TABLE IF NOT EXISTS HARPA.data_center_staging (LIKE HARPA.data_center INCLUDING ALL);
CREATE TEMP TABLE IF NOT EXISTS HARPA.fotovoltaico_staging (LIKE HARPA.fotovoltaico INCLUDING ALL);

-- Passo 2: Caricamento dei dati dai CSV aggiornati nelle tabelle di staging
COPY HARPA.edificio_staging (data, kilowatt) FROM '/csv/Generale_Edificio_Energia_Attiva.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');
COPY HARPA.data_center_staging (data, kilowatt) FROM '/csv/Generale_Data_Center_Energia_Attiva.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');
COPY HARPA.fotovoltaico_staging (data, kilowatt) FROM '/csv/Impianto_Fotovoltaico_Energia_Attiva_Prodotta.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');

-- Passo 3: Aggiornamento delle tabelle principali con i dati della tabella di staging
-- Esempio di aggiornamento o inserimento di dati nella tabella edificio
INSERT INTO HARPA.edificio (data, kilowatt)
SELECT data, kilowatt FROM HARPA.edificio_staging
ON CONFLICT (data) DO UPDATE SET kilowatt = EXCLUDED.kilowatt;

-- Ripeti per le altre tabelle (data_center, fotovoltaico)
INSERT INTO HARPA.data_center (data, kilowatt)
SELECT data, kilowatt FROM HARPA.data_center_staging
ON CONFLICT (data) DO UPDATE SET kilowatt = EXCLUDED.kilowatt;

INSERT INTO HARPA.fotovoltaico (data, kilowatt)
SELECT data, kilowatt FROM HARPA.fotovoltaico_staging
ON CONFLICT (data) DO UPDATE SET kilowatt = EXCLUDED.kilowatt;




-- Aggiornamento della tabella ufficio utilizzando i dati aggregati dalle tabelle principali
-- (Assumendo che la tabella ufficio sia derivata dalle altre tabelle)
INSERT INTO HARPA.ufficio (data, kilowatt)
SELECT DISTINCT COALESCE(e.data, dc.data, f.data) AS data,
    (COALESCE(e.kilowatt, 0) - COALESCE(dc.kilowatt, 0) + COALESCE(f.kilowatt, 0)) AS kilowatt
FROM HARPA.edificio_staging e
FULL OUTER JOIN HARPA.data_center_staging dc ON e.data = dc.data
FULL OUTER JOIN HARPA.fotovoltaico_staging f ON COALESCE(e.data, dc.data) = f.data
ON CONFLICT (data) DO UPDATE SET kilowatt = EXCLUDED.kilowatt;



-- Inserimento dei dati unificati nella tabella unione_dataset, con il calcolo aggiornato per kilowatt_ufficio
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
ORDER BY data;




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
ORDER BY data;




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
ORDER BY data;




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
ORDER BY primo_giorno_mese;



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
ORDER BY primo_giorno_anno;



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
    DATE(giorni.primo) AS data,  -- Convertito in DATE
    (ultimo_valore.kilowatt_edificio - primo_valore.kilowatt_edificio) AS kilowatt_edificio,
    (ultimo_valore.kilowatt_data_center - primo_valore.kilowatt_data_center) AS kilowatt_data_center,
    (ultimo_valore.kilowatt_fotovoltaico - primo_valore.kilowatt_fotovoltaico) AS kilowatt_fotovoltaico,
    (ultimo_valore.kilowatt_ufficio - primo_valore.kilowatt_ufficio) AS kilowatt_ufficio,
    giorni.fascia_oraria,
    giorni.giorno_settimana
FROM
    (SELECT DATE(data) AS primo, fascia_oraria, giorno_settimana  -- Convertito in DATE
     FROM HARPA.unione_dataset
     GROUP BY DATE(data), fascia_oraria, giorno_settimana) AS giorni  -- Group by DATE(data)
JOIN
    (SELECT data, kilowatt_edificio, kilowatt_data_center, kilowatt_fotovoltaico, kilowatt_ufficio, fascia_oraria, giorno_settimana
     FROM HARPA.unione_dataset
     WHERE (data, fascia_oraria, giorno_settimana) IN
         (SELECT MIN(data), fascia_oraria, giorno_settimana
          FROM HARPA.unione_dataset
          GROUP BY DATE(data), fascia_oraria, giorno_settimana)) AS primo_valore  -- Group by DATE(data)
ON DATE(giorni.primo) = DATE(primo_valore.data)  -- Convertito in DATE
AND giorni.fascia_oraria = primo_valore.fascia_oraria
AND giorni.giorno_settimana = primo_valore.giorno_settimana
JOIN
    (SELECT data, kilowatt_edificio, kilowatt_data_center, kilowatt_fotovoltaico, kilowatt_ufficio, fascia_oraria, giorno_settimana
     FROM HARPA.unione_dataset
     WHERE (data, fascia_oraria, giorno_settimana) IN
         (SELECT MAX(data), fascia_oraria, giorno_settimana
          FROM HARPA.unione_dataset
          GROUP BY DATE(data), fascia_oraria, giorno_settimana)) AS ultimo_valore  -- Group by DATE(data)
ON DATE(giorni.primo) = DATE(ultimo_valore.data)  -- Convertito in DATE
AND giorni.fascia_oraria = ultimo_valore.fascia_oraria
AND giorni.giorno_settimana = ultimo_valore.giorno_settimana
ORDER BY DATE(giorni.primo), giorni.fascia_oraria, giorni.giorno_settimana;