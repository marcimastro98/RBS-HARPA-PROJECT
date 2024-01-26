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
                fascia_oraria, 
                kilowatt_ufficio_diff,
                EXTRACT(DOW FROM giorno) AS giorno_settimana_numerico
            FROM harpa.aggregazione_fascia_oraria
            ORDER BY giorno;
        """
        cur.execute(aggregazione_oraria_query)
        df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])

        # Filtra il DataFrame per selezionare solo le righe dove 'fascia_oraria' è '09:00-18:00'
        df_filtrato = df[df['fascia_oraria'] == '09:00-18:00']

        # Calcola la media della colonna 'kilowatt_ufficio_diff' per il DataFrame filtrato
        media_kilowatt = df_filtrato['kilowatt_ufficio_diff'].mean()

        # Applicazione di una espressione lambda che usa if-elif-else direttamente
        df['is_smartworking'] = df.apply(
            lambda row: 'NA' if pd.isnull(row['kilowatt_ufficio_diff'])
            else 'OK' if row['kilowatt_ufficio_diff'] > media_kilowatt and row['fascia_oraria'] == '09:00-18:00'
            else 'KO',
            axis=1
        )
        # Aggiornamento delle tabelle con i valori 'is_smartworking'
        update_queries = {
            'aggregazione_ora': """
                UPDATE harpa.aggregazione_ora
                SET is_smartworking = %s
                WHERE giorno = %s AND fascia_oraria = %s;
            """,
            'aggregazione_giorno': """
                UPDATE harpa.aggregazione_giorno
                SET is_smartworking = %s
                WHERE giorno = %s;
            """,
            'aggregazione_fascia_oraria': """
                UPDATE harpa.aggregazione_fascia_oraria
                SET is_smartworking = %s
                WHERE giorno = %s AND fascia_oraria = %s;
            """
        }

        # Prepara i dati per l'aggiornamento batch
        update_data_aggregazione_ora = []
        update_data_aggregazione_giorno = []
        update_data_aggregazione_fascia_oraria = []

        for i, row in df.iterrows():
            update_data_aggregazione_ora.append((row['is_smartworking'], row['giorno'], row['fascia_oraria']))
            update_data_aggregazione_giorno.append((row['is_smartworking'], row['giorno']))
            update_data_aggregazione_fascia_oraria.append((row['is_smartworking'], row['giorno'], row['fascia_oraria']))

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
                    TO_CHAR(DATE_TRUNC('month', giorno), 'YYYY-MM') AS mese_troncato,  -- Converti il timestamp in una stringa 'YYYY-MM'
                    TO_CHAR(ROUND((COUNT(*) FILTER (WHERE is_smartworking = 'OK')::NUMERIC / COUNT(*)) * 100, 2), 'FM9990.00') || '%' AS percentuale_smart_working
                FROM harpa.aggregazione_giorno
                GROUP BY DATE_TRUNC('month', giorno)
            ) AS subquery
            WHERE harpa.aggregazione_mese.mese = subquery.mese_troncato;  -- Confronta la stringa 'YYYY-MM' con un'altra stringa 'YYYY-MM'
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
