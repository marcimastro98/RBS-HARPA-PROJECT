# RBS-HARPA-PROJECT
- Allo start del progetto viene creata la cartella dataset_result <b>IMPORTANTE mettere i dataset da analizzare dentro la cartella 'Dataset'</b>


#### Dataset:

- Dataset: troverete i file csv originali forniti da Andrea.

- dataset_result: Cartella creata in automatico con sottocartelle in base all'operazione effettuata, dentro le successive cartelle i dataset prodotti si divideranno per ORA, GIORNO, MESE, ANNO

Dentro la cartella meteo, ci sono dati meteo scaricati dall'API

 ``````
    https://open-meteo.com/en/docs/historical-weather-api. 
``````

Infine i file dentro la cartella dataset_result sono i tabelle dove troviamo i kilowatt consumati per giorni e per mese.

#### File Python:

- main: Codice che serve per la creazione dei file csv all'interno della cartella meteo_consumption e quindi delle tabelle di join fra dati meteo e consumi, inoltre da qui viene runnato il progetto.

- calculate_consumption: Qui troviamo la logica che serve per la creazioni dei file csv che calcola i giorni e il mese sulla base dei csv originali forniti da Andrea.

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

---

#### Esecuzione su Python

````
## PIP install
pip install --no-cache-dir -r requirements.txt
````

````
python main.py
````

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

#### Come contribuire al progetto

 - Fork it (https://github.com/marcimastro98/RBS-HARPA-PROJECT)
 - Crea un nuovo ramo con le feature che vuoi includere (git checkout -b **feature/fooBar**)
 - Commit delle modifiche (git commit -am 'Add some fooBar')
 - Push del nuovo ramo (git push origin **feature/fooBar**)
 - Crea una nuova *Pull Request*
