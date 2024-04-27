# cron_update_table.sh Script
<br>Il file `cron_update_table.sh` è progettato per eseguire automaticamente operazioni di pulizia e inserimento dei dati nel database, oltre all'addestramento mensile, utilizzando `docker-compose`.
<br> Inoltre la seguente guida spiega come salvare i dati nel database delle previsioni future ogni 7 giorni.

## Descrizione

Questo script è configurato per essere eseguito automaticamente il primo giorno di ogni mese, garantendo che i dati e i modelli siano costantemente aggiornati.

## Prerequisiti

Prima di programmare l'esecuzione dello script, assicurati di avere installato:

- **Docker**: Installa Docker sul tuo sistema. Per istruzioni dettagliate, visita la [pagina di installazione di Docker](https://docs.docker.com/get-docker/).
- **Python**: Assicurati di avere installato Python, almeno versione 3.12. Puoi scaricarlo da [Python.org](https://www.python.org/downloads/).

## Configurazione dello script

Per configurare lo script per l'esecuzione automatizzata, segui questi passi:

1. Assegna i permessi di esecuzione allo script:
```bash
chmod +x /script_sh/cron_update_table.sh
```
2. Apri il crontab per editare i cron jobs:
```bash
crontab -e  # su MacOS
```
3. Aggiungi la seguente riga al crontab per programmare lo script alle 00:01 del primo giorno di ogni mese:
```bash
1 0 1 * * /script_sh/cron_update_table.sh
```
4. Aggiungi la seguente riga al crontab per programmare la previsione dei futuri consumi ogni 7 giorni:
```bash
0 0 * * 0 python3 /pyscript/predict_future_consumption.py
```

5. Salva le modifiche nel crontab:
- Se stai usando vi, premi ESC, digita :wq, e premi ENTER.
- Se stai usando nano, premi CTRL+X, poi Y e infine ENTER.

## Verifica della configurazione
Per verificare che il cron job sia stato configurato correttamente, esegui:
```bash
crontab -l
```
