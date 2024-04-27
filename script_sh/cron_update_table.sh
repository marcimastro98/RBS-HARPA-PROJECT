#!/bin/bash

# Percorso al file docker-compose.yml
DOCKER_COMPOSE_FILE="../Docker/docker-compose.yml"

# Nome del container da monitorare
CONTAINER_NAME="python-app-1"

# Fermare e rimuovere tutti i container attivi
echo "Fermo e rimuovo tutti i container..."
docker-compose -f "$DOCKER_COMPOSE_FILE" down

# Rimozione di tutti i container (aggiunto per maggiore certezza)
docker rm $(docker ps -a -q) 2> /dev/null

# Rimozione di tutti i volumi Docker
echo "Rimozione di tutti i volumi Docker..."
docker volume rm $(docker volume ls -q) 2> /dev/null

# Avvia il container
echo "Avvio del container $CONTAINER_NAME..."
docker-compose -f "$DOCKER_COMPOSE_FILE" up -d

# Controlla se tutti i container sono avviati e in esecuzione
echo "Controllo dello stato dei container..."
CONTAINER_UP="no"
while [ $CONTAINER_UP != "yes" ]; do
  # Controlla lo stato di tutti i container, se sono "healthy" o meno.
  if docker ps | grep -w "$CONTAINER_NAME" &> /dev/null; then
    CONTAINER_UP="yes"
  else
    echo "In attesa che il container sia avviato..."
    sleep 10
  fi
done

echo "Tutti i container sono attivi. In attesa che $CONTAINER_NAME completi..."

# Attesa che il container $CONTAINER_NAME termini l'esecuzione
while [ $(docker inspect -f '{{.State.Running}}' $CONTAINER_NAME) == "true" ]; do
  echo "In attesa che il container $CONTAINER_NAME termini..."
  sleep 10
done

# Verificare se il container è terminato senza errori
EXIT_CODE=$(docker inspect -f '{{.State.ExitCode}}' $CONTAINER_NAME)
if [ $EXIT_CODE -eq 0 ]; then
  echo "Il container $CONTAINER_NAME è terminato correttamente."
  # Attesa di 30 minuti
  echo "In attesa 30 minuti prima di eseguire lo script Python..."
  sleep 1800  # Attesa di 30 minuti

  # Eseguire lo script Python
  echo "Esecuzione dello script train_ensemble_model.py..."
  python3 ../machine_learning/train_ensemble_model.py
else
  echo "Il container $CONTAINER_NAME è terminato con errore: $EXIT_CODE"
fi