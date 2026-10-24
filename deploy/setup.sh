#!/usr/bin/env bash
# Prepara una instancia EC2 (Ubuntu 24.04) o una VM de GCP para correr
# DFSha. El Learner Lab apaga las instancias al cerrar sesion, asi que vas
# a ejecutar esto muchas veces. No lo hagas a mano.
#
# Equivale al paso 5 del taller de nginx (instalar docker y git), con el
# repositorio oficial de Docker para tener el plugin "docker compose".
set -e

sudo apt-get update
sudo apt-get install -y ca-certificates curl git

sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
     -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] \
https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo $VERSION_CODENAME) stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io \
     docker-buildx-plugin docker-compose-plugin

sudo systemctl enable docker
sudo systemctl start docker
sudo usermod -a -G docker "$USER"

echo
echo "Docker instalado. Cierra la sesion SSH y vuelve a entrar para que"
echo "el grupo docker tome efecto. Despues sigue docs/despliegue.md."
echo
echo "Puertos a abrir en el Security Group / firewall:"
echo "  50051/tcp  NameNode  (clientes y DataNodes)"
echo "  50052/tcp  Raft      (solo entre NameNodes)"
echo "  50060/tcp  DataNode  (clientes y otros DataNodes)"
