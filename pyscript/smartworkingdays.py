from decimal import Decimal

import pandas as pd


def smartworking_insert(cur):
    try:
        # Inizia una transazione
        cur.execute("BEGIN;")

        schema_name = 'harpa'
        tables_to_update = ['aggregazione_ora', 'aggregazione_giorno', 'aggregazione_fascia_oraria',
                            'aggregazione_mese', 'aggregazione_anno']
        for table in tables_to_update:
            cur.execute(f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT FROM information_schema.columns 
                        WHERE table_schema = '{schema_name}' AND table_name = '{table}' AND column_name = 'is_smartworking'
                    ) THEN
                        ALTER TABLE {schema_name}.{table} ADD COLUMN is_smartworking TEXT;
                    END IF;
                END
                $$;
            """)

        # Esecuzione della query principale e costruzione del DataFrame
        aggregazione_oraria_query = """
            SELECT
                giorno, 
                giorno_settimana, 
                fascia_oraria_num, 
                kilowatt_ufficio_diff,
                EXTRACT(DOW FROM giorno) AS giorno_settimana_numerico
            FROM harpa.aggregazione_fascia_oraria
            ORDER BY giorno;
        """
        cur.execute(aggregazione_oraria_query)
        df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])

        # Filtro il df per prendere solo le fasce orarie comprese tra le 09:00-19:00 solo i sabati e domeniche
        df_filtrato_sabato_domenica = df[
            ((df['giorno_settimana_numerico'] == 6) | (df['giorno_settimana_numerico'] == 0))
            & (df['fascia_oraria_num'] == 2)]
        # Filtro il df per prendere solo le fasce orarie comprese tra le 09:00-19:00 esclusi sabati e domeniche
        df_filtrato_lavorativi = df[(df['giorno_settimana_numerico'] != 6)
                                    & (df['giorno_settimana_numerico'] != 0)
                                    & (df['fascia_oraria_num'] == 2)]
        # Calcolo la media dei kilowatti di sabato e domenica
        media_kilowatt_sabato_domenica = df_filtrato_sabato_domenica['kilowatt_ufficio_diff'].abs().mean()
        # Calcolo la media dei kilowatti esclusi sabato e domenica
        media_kilowatt_lavorativi = df_filtrato_lavorativi['kilowatt_ufficio_diff'].abs().mean()
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
            'KO' if row['kilowatt_ufficio_diff'] > media_kilowatt
                    and row['fascia_oraria_num'] == 2
                    and row['giorno_settimana_numerico'] != 6
                    and row['giorno_settimana_numerico'] != 0
            else 'OK' if row['kilowatt_ufficio_diff'] < media_kilowatt
                         and row['fascia_oraria_num'] == 2
                         and row['giorno_settimana_numerico'] != 6
                         and row['giorno_settimana_numerico'] != 0
            else 'NA',
            axis=1
        )
        # Aggiornamento delle tabelle con i valori 'is_smartworking'
        update_queries = {
            'aggregazione_ora': """
                UPDATE harpa.aggregazione_ora
                SET is_smartworking = %s
                WHERE giorno = %s AND fascia_oraria_num = %s;
            """,
            'aggregazione_giorno': """
                UPDATE harpa.aggregazione_giorno
                SET is_smartworking = %s
                WHERE giorno = %s;
            """,
            'aggregazione_fascia_oraria': """
                UPDATE harpa.aggregazione_fascia_oraria
                SET is_smartworking = %s
                WHERE giorno = %s AND fascia_oraria_num = %s;
            """
        }

        # Prepara i dati per l'aggiornamento batch
        update_data_aggregazione_ora = []
        update_data_aggregazione_giorno = []
        update_data_aggregazione_fascia_oraria = []

        for i, row in df.iterrows():
            update_data_aggregazione_ora.append((row['is_smartworking'], row['giorno'], row['fascia_oraria_num']))
            update_data_aggregazione_giorno.append((row['is_smartworking'], row['giorno']))
            update_data_aggregazione_fascia_oraria.append(
                (row['is_smartworking'], row['giorno'], row['fascia_oraria_num']))

        # Esegui gli aggiornamenti in batch
        cur.executemany(update_queries['aggregazione_ora'], update_data_aggregazione_ora)
        cur.executemany(update_queries['aggregazione_giorno'], update_data_aggregazione_giorno)
        cur.executemany(update_queries['aggregazione_fascia_oraria'], update_data_aggregazione_fascia_oraria)

        # Inserimento dei dati aggregati nelle tabelle mensili e annuali
        cur.execute("""
            UPDATE harpa.aggregazione_mese
            SET is_smartworking = subquery.percentuale_smart_working
            FROM (
                SELECT
                    TO_CHAR(DATE_TRUNC('month', giorno), 'YYYY-MM') AS mese_troncato,
                    TO_CHAR(ROUND((COUNT(*) FILTER (WHERE is_smartworking = 'OK')::NUMERIC / COUNT(*)) * 100, 2), 'FM9990.00') || '%' AS percentuale_smart_working
                FROM harpa.aggregazione_giorno
                GROUP BY DATE_TRUNC('month', giorno)
            ) AS subquery
            WHERE harpa.aggregazione_mese.mese = subquery.mese_troncato;
        """)
        cur.execute("""
            UPDATE harpa.aggregazione_anno
            SET is_smartworking = subquery.percentuale_smart_working
            FROM (
                SELECT
                    EXTRACT(YEAR FROM giorno) AS anno,
                    TO_CHAR(ROUND((COUNT(*) FILTER (WHERE is_smartworking = 'OK')::NUMERIC / COUNT(*)) * 100, 2), 'FM9990.00')
                     || '%' AS percentuale_smart_working FROM harpa.aggregazione_giorno
                GROUP BY EXTRACT(YEAR FROM giorno)
            ) AS subquery
            WHERE harpa.aggregazione_anno.anno = subquery.anno;
        """)

        # Commit delle modifiche
        cur.execute("COMMIT;")
        print("Added smartworking days to tables!")
    except Exception as e:
        # In caso di errore, effettua il rollback
        cur.execute("ROLLBACK;")
        print(f"Errore nell'aggiunta dello smart working: {e}")
        raise
