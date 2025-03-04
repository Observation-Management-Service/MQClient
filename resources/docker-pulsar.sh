#!/bin/bash
set -euo pipefail

echo "--------------------------------------------------------------"
echo "starting pulsar broker..."

if [ -z "${1-}" ]; then
    echo "MISSING ARG: docker-pulsar.sh CONTAINER_NAME"
    exit 1
fi

DOCKERIZE_VERSION=v0.6.1

wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz && sudo tar -C /usr/local/bin -xzvf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz && rm dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz

docker run -i --rm --name $1 \
    -p 6650:6650 \
    -p 8080:8080 \
    apachepulsar/pulsar:2.6.0 /bin/bash \
    -c "sed -i s/brokerDeleteInactiveTopicsEnabled=.*/brokerDeleteInactiveTopicsEnabled=false/ /pulsar/conf/standalone.conf && bin/pulsar standalone" \
    >>broker.out 2>&1 &
dockerize -wait tcp://localhost:8080 -timeout 10m
dockerize -wait tcp://localhost:6650 -timeout 10m

echo "--------------------------------------------------------------"
echo "waiting for pulsar broker..."
sleep 30 # pulsar takes a while to launch...
