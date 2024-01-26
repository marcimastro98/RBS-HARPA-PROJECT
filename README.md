# RBS-HARPA-PROJECT
- Per startare il progetto è necessario installare Docker sul proprio pc. Una volta scaricato il progetto e scaricato docker posizionarsi all'interno della directory Docker e lanciare in console il comando 'docker-compose up -d', questo crea i container relativi a pgadmin e postgres, inoltre si occupa di eseguire la query(che si trova nella cartella .init/init-db.sql) di creazione del database importando i csv contenuti all'interno della cartella Dataset, quindi, crea le tabelle. Infine, con questo comando, viene lanciato lo script python contenuto nella cartella pyscript (main.py) <b>IMPORTANTE mettere i dataset da analizzare dentro la cartella 'Dataset'</b>. E' possibile visualizzare le tabelle del database da pgadmin dopo avere avviato i container docker, collegandosi all'indirizzo http://localhost:5050.


#### Dataset:

- Dataset: troverete i file csv originali forniti da Andrea.

#### API:

 ``````
    https://open-meteo.com/en/docs/historical-weather-api. 
``````
#### Database:
All'interno del database trovete diverse tabelle fra cui:
- Edificio: Contiene i dati originali del csv relativo all'edificio
- Datacenter: Contiene i dati originali del csv relativo al Datacenter
- Fotovoltaico: Contiene i dati originali del csv relativo al fotovoltaico
- aggregazione_ora: Tabelle relativa alla media dei kilowatt delle strutture per ora e dei dati meteo (temperatura, pioggia in millimetri, copertura del cielo in %)
- aggregazione_fascia_oraria: Tabelle relativa alla media dei kilowatt delle strutture per fascia oraria (00:00 - 09:00, 09:00 - 18:00, 18:00 - 00:00) e dei dati meteo (temperatura, pioggia in millimetri, copertura del cielo in %)
- aggregazione_giorno: Tabelle relativa alla media dei kilowatt delle strutture per giorno e dei dati meteo (temperatura, pioggia in millimetri, copertura del cielo in %)
- _aggregazione_mese: Tabelle relativa alla media dei kilowatt delle strutture per mese e dei dati meteo (temperatura, pioggia in millimetri, copertura del cielo in %)
- aggregazione_anno: Tabelle relativa alla media dei kilowatt delle strutture per anno e dei dati meteo (temperatura, pioggia in millimetri, copertura del cielo in %)


#### File Python:

- main: Codice che serve per stabilire la connessione al db e per fare l'update delle tabelle 

- meteo_table: Qui troviamo la logica che serve per fare l'update delle tabelle (calcolo dei kilowatt in base al periodo, calcolo dei kilowatt relativi al consumo degli uffici, aggiunta dei dati meteo)

- meteoAPI: Codice che serve chiamare le API di Open-Meteo e fornirci e caricare i dati meteo su dataset historical_meteo.csv


#### Info Generali:

![img.png](img.png)

#### Dipendenze 

| # | Nome               | Versione | 
|---|--------------------|----------|
| 1 | openmeteo_requests | 1.1.0    | 
| 2 | pandas             | 2.0.3    | 
| 3 | requests_cache     | 1.1.1    |
| 4 | retry_requests     | 2.0.0    |
| 5 | psycopg2           | 2.9.9    |
| 6 | python_dotenv      | 1.0.0    |



---

### Esecuzione applicativo Python

#### Esecuzione su Python 

````
## PIP install
pip install --no-cache-dir -r requirements.txt
````

````
python main.py
````
#### Accesso al db da pgadmin
Una volta collegati a localhost:5050 inserire admin@admin.com e psw:root, tasto destri su Servers -> Register, name: HARPA, Connection -> Host Name: db, Username: user, Password: password



Parte da definire se lasciarla o no 
#### Esecuzione su Docker

````
## Build image
docker build -t localhost/rbs-harpa-project:latest --load -f Dockerfile .
````

````
## Run container
docker run --rm --name test -v ./dataset_result:/app/dataset_result:rw,z  localhost/rbs-harpa-project:latest 
````


#### Esecuzione su Podman

````
## Build image
podman build -t localhost/rbs-harpa-project:latest -f Containerfile .
````

````
## Run container
podman run --rm --name test -v ./dataset_result:/app/dataset_result:rw,z  localhost/rbs-harpa-project:latest 
````
---

### Esecuzione  SQL (Postgres)

#### Esecuzione su Podman

````
podman run --name postgres -e POSTGRES_USER=harpa -e POSTGRES_PASSWORD=harpa -p 5432:5432 -v $PWD/Dataset/pg_data -d postgres
````
Caricare i dati nel database utilizzando i seguenti file.

- Dataset/Generale_Data_Center_Energia_Attiva.sql
- Dataset/Generale_Edificio_Energia_Attiva.sql
- Dataset/Impianto_Fotovoltaico_Energia_Attiva_Prodotta.sql

Successivamente eseguire lo script SQL "dataset_result/SQL/script_SQL_merge_dataset.sql" per creare l'analisi finale.

### Come contribuire al progetto

 - Fork it (https://github.com/marcimastro98/RBS-HARPA-PROJECT)
 - Crea un nuovo ramo con le feature che vuoi includere (git checkout -b **feature/fooBar**)
 - Commit delle modifiche (git commit -am 'Add some fooBar')
 - Push del nuovo ramo (git push origin **feature/fooBar**)
 - Crea una nuova *Pull Request*
