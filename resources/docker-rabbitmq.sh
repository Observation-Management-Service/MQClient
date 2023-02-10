#!/bin/bash

echo "--------------------------------------------------------------"
echo "starting rabbitmq broker..."

DOCKERIZE_VERSION=v0.6.1

wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz && sudo tar -C /usr/local/bin -xzvf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz && rm dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz

if [ -z $1 ]; then
    MOUNTS=""
else
    MOUNTS="--mount type=bind,source={$(dirname $(realpath $1))},target=/etc/rabbitmq"
fi

set -x
docker run -i -d --rm \
    -p 5672:5672 \
    -p 15672:15672 \
    $MOUNTS \
    deadtrickster/rabbitmq_prometheus:3.7
dockerize -wait tcp://localhost:5672 -timeout 10m

echo "--------------------------------------------------------------"
echo "waiting for rabbitmq broker..."
sleep 15