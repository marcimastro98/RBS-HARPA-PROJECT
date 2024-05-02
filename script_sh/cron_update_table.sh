#!/bin/bash

# Percorso al file docker-compose.yml
DOCKER_COMPOSE_FILE="../Docker/docker-compose.yml"

# Nome del container principale da monitorare
CONTAINER_NAME="python-app-1"

# Nome del container per il training del modello
MODEL_CONTAINER_NAME="predictions"

# Fermare e rimuovere tutti i container attivi
echo "Fermo e rimuovo tutti i container..."
docker-compose -f "$DOCKER_COMPOSE_FILE" down

# Rimozione di tutti i container (aggiunto per maggiore certezza)
docker rm $(docker ps -a -q) 2> /dev/null

# Rimozione di tutti i volumi Docker
echo "Rimozione di tutti i volumi Docker..."
docker volume rm $(docker volume ls -q) 2> /dev/null

# Avvia tutti i container tranne il container per il training
echo "Avvio di tutti i container tranne $MODEL_CONTAINER_NAME..."
docker-compose -f "$DOCKER_COMPOSE_FILE" up -d --scale $MODEL_CONTAINER_NAME=0

# Controlla se tutti i container tranne quello per il training sono avviati e in esecuzione
echo "Controllo dello stato dei container..."
while [ $(docker inspect -f '{{.State.Running}}' $MODEL_CONTAINER_NAME) == "false" ]; do
  if [ $(docker ps -q | wc -l) -eq 0 ]; then
    echo "In attesa che i container siano tutti avviati..."
    sleep 10
  else
    break
  fi
done

echo "Tutti i container tranne $MODEL_CONTAINER_NAME sono attivi. In attesa che $CONTAINER_NAME completi..."

# Attesa che il container $CONTAINER_NAME termini l'esecuzione
while [ $(docker inspect -f '{{.State.Running}}' $CONTAINER_NAME) == "true" ]; do
  echo "In attesa che il container $CONTAINER_NAME termini..."
  sleep 10
done

# Verificare se il container $CONTAINER_NAME è terminato senza errori
EXIT_CODE=$(docker inspect -f '{{.State.ExitCode}}' $CONTAINER_NAME)
if [ $EXIT_CODE -eq 0 ]; then
  echo "Il container $CONTAINER_NAME è terminato correttamente."
  echo "Avvio del container $MODEL_CONTAINER_NAME per il training del modello..."
  docker-compose -f "$DOCKER_COMPOSE_FILE" up -d $MODEL_CONTAINER_NAME
else
  echo "Il container $CONTAINER_NAME è terminato con errore: $EXIT_CODE"
fi
