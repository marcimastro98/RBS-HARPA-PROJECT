#!/bin/bash

# Percorso al file docker-compose.yml
DOCKER_COMPOSE_FILE="../Docker/docker-compose-yml"

# Nome del container da avviare
CONTAINER_NAME="docker-python-app-1"

# Avvia il container
echo "Avvio del container $CONTAINER_NAME"
docker-compose -f "$DOCKER_COMPOSE_FILE" up -d
