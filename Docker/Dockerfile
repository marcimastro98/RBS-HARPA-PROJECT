# Usa un'immagine base di Python
FROM python:3.9

# Imposta una directory di lavoro nel container
WORKDIR /app

# Copia il file requirements e installa le dipendenze
COPY ./pyscript/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copia il codice sorgente Python nel container
COPY ./pyscript/ ./

# Copia il file .env dal percorso appropriato
COPY ./env/ /app/env/


# Comando per eseguire lo script
CMD ["python", "main.py"]
