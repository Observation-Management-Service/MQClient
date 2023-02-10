#!/bin/bash

echo "--------------------------------------------------------------"
echo "starting rabbitmq broker..."

DOCKERIZE_VERSION=v0.6.1

wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz && sudo tar -C /usr/local/bin -xzvf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz && rm dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz

if [ -z $1 ]; then
    MOUNTS=""
else
    MOUNTS="--v $(realpath $1):/bitnami/rabbitmq/conf/custom.conf:ro"
fi

set -x
docker run -i -d --rm \
    -p 5672:5672 \
    -p 15672:15672 \
    $MOUNTS \
    bitnami/rabbitmq:latest
dockerize -wait tcp://localhost:5672 -timeout 10m

echo "--------------------------------------------------------------"
echo "waiting for rabbitmq broker..."
sleep 15