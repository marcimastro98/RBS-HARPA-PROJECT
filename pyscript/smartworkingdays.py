from decimal import Decimal

import pandas as pd
import psycopg2


def smartworking_insert(cur, logging):
    try:
        # Inizia una transazione
        cur.execute("BEGIN;")
        logging.info("Inizio transazione per l'aggiornamento smartworking.")

        schema_name = 'HARPA'
        tables_to_update = ['aggregazione_giorno', 'aggregazione_fascia_oraria',
                            'aggregazione_mese', 'aggregazione_anno']

        for table in tables_to_update:
            logging.info(f"Verifica e aggiunta della colonna is_smartworking alla tabella {table}.")
            cur.execute(f"""
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT FROM information_schema.columns 
                            WHERE table_schema = '{schema_name}' AND table_name = '{table}' AND column_name = 'is_smartworking'
                        ) THEN
                            ALTER TABLE {schema_name}.{table} ADD COLUMN is_smartworking TEXT;
                            RAISE NOTICE 'Aggiunta colonna is_smartworking alla tabella {table}.';
                        END IF;
                    END
                    $$;
                """)

        logging.info("Query principale per aggregazione oraria.")
        aggregazione_oraria_query = f"""
                SELECT
                    data, 
                    giorno_settimana, 
                    fascia_oraria, 
                    kilowatt_ufficio
                FROM {schema_name}.aggregazione_fascia_oraria
                ORDER BY data;
            """
        cur.execute(aggregazione_oraria_query)
        results = cur.fetchall()
        if not results:
            logging.warning("Nessun dato trovato per l'aggregazione oraria. Interrompo l'aggiornamento smartworking.")
            cur.execute("ROLLBACK;")
            return

        df = pd.DataFrame(results, columns=[desc[0] for desc in cur.description])
        df = df.drop(df[(df['kilowatt_ufficio'].isnull()) |
                        (df['kilowatt_ufficio'] == 0)].index)

        # Filtro il df per prendere solo le fasce orarie comprese tra le 09:00-19:00 solo i sabati e domeniche
        df_filtrato_sabato_domenica = df[
            ((df['giorno_settimana'] == 6) | (df['giorno_settimana'] == 0))
            & (df['fascia_oraria'] == 2)]
        # Filtro il df per prendere solo le fasce orarie comprese tra le 09:00-19:00 esclusi sabati e domeniche
        df_filtrato_lavorativi = df[(df['giorno_settimana'] != 6)
                                    & (df['giorno_settimana'] != 0)
                                    & (df['fascia_oraria'] == 2)]
        # Calcolo la media dei kilowatti di sabato e domenica
        media_kilowatt_sabato_domenica = df_filtrato_sabato_domenica['kilowatt_ufficio'].abs().mean()
        # Calcolo la media dei kilowatti esclusi sabato e domenica
        media_kilowatt_lavorativi = df_filtrato_lavorativi['kilowatt_ufficio'].abs().mean()
        # Divido la media dei kilowatt dei giorni lavorativi diviso 57(numero di persone)
        # così trovo i kilowatt consumati dal singolo individuo
        # sto levando dai giorni lavorativi i kilowatt sempre consumati senza persone il sabato e la domenica
        media_kilowatt_lavorativi_a_persona = (media_kilowatt_lavorativi - media_kilowatt_sabato_domenica) / 57
        # Calcolo la media facendo la media dei kilowatt di sabato e domenica
        # + la media dei giorni lavorativi a persona per 10 persone(numero di persone sempre presenti in ufficio)
        media_kilowatt = Decimal(str(media_kilowatt_sabato_domenica + (media_kilowatt_lavorativi_a_persona * 10)))
        # Applicazione di una espressione lambda che usa if-elif-else direttamente
        df['is_smartworking'] = df.apply(
            lambda row:
            'KO' if row['kilowatt_ufficio'] > media_kilowatt
                    and row['fascia_oraria'] == 2
                    and row['giorno_settimana'] != 6
                    and row['giorno_settimana'] != 0
            else 'OK' if row['kilowatt_ufficio'] < media_kilowatt
                         and row['fascia_oraria'] == 2
                         and row['giorno_settimana'] != 6
                         and row['giorno_settimana'] != 0
            else 'NA',
            axis=1
        )
        # Aggiornamento delle tabelle con i valori 'is_smartworking'
        update_queries = {
            'aggregazione_giorno': """
                UPDATE HARPA.aggregazione_giorno
                SET is_smartworking = %s
                WHERE data = %s;
            """,
            'aggregazione_fascia_oraria': """
                UPDATE HARPA.aggregazione_fascia_oraria
                SET is_smartworking = %s
                WHERE data = %s AND fascia_oraria = %s;
            """
        }

        # Prepara i dati per l'aggiornamento batch
        update_data_aggregazione_giorno = []
        update_data_aggregazione_fascia_oraria = []

        for i, row in df.iterrows():
            update_data_aggregazione_giorno.append((row['is_smartworking'], row['data']))
            update_data_aggregazione_fascia_oraria.append(
                (row['is_smartworking'], row['data'], row['fascia_oraria']))

        # Esegui gli aggiornamenti in batch
        cur.executemany(update_queries['aggregazione_giorno'], update_data_aggregazione_giorno)
        cur.executemany(update_queries['aggregazione_fascia_oraria'], update_data_aggregazione_fascia_oraria)

        # Inserimento dei dati aggregati nelle tabelle mensili e annuali
        cur.execute("""
            UPDATE HARPA.aggregazione_mese
            SET is_smartworking = subquery.percentuale_smart_working
            FROM (
                SELECT
                    TO_CHAR(DATE_TRUNC('month', data), 'YYYY-MM') AS mese_troncato,
                    TO_CHAR(ROUND((COUNT(*) FILTER (WHERE is_smartworking = 'OK')::NUMERIC / COUNT(*)) * 100, 2), 'FM9990.00') || '%' AS percentuale_smart_working
                FROM HARPA.aggregazione_giorno
                GROUP BY DATE_TRUNC('month', data)
            ) AS subquery
            WHERE TO_CHAR(HARPA.aggregazione_mese.data, 'YYYY-MM') = subquery.mese_troncato;
        """)
        cur.execute("""
            UPDATE HARPA.aggregazione_anno
            SET is_smartworking = subquery.percentuale_smart_working
            FROM (
                SELECT
                    EXTRACT(YEAR FROM data) AS anno,
                    TO_CHAR(ROUND((COUNT(*) FILTER (WHERE is_smartworking = 'OK')::NUMERIC / COUNT(*)) * 100, 2), 'FM9990.00')
                     || '%' AS percentuale_smart_working FROM HARPA.aggregazione_giorno
                GROUP BY EXTRACT(YEAR FROM data)
            ) AS subquery
            WHERE EXTRACT(YEAR FROM HARPA.aggregazione_anno.data) = subquery.anno;
        """)

        # Commit delle modifiche
        cur.execute("COMMIT;")
    except psycopg2.Error as e:
        cur.execute("ROLLBACK;")
        logging.error(f"Errore psycopg2 durante l'aggiunta dello smart working: {e}")
    except Exception as e:
        cur.execute("ROLLBACK;")
        logging.error(f"Errore generico durante l'aggiunta dello smart working: {e}")
    else:
        cur.execute("COMMIT;")
        logging.info("Transazione completata con successo. Giornate di smart working aggiornate.")
