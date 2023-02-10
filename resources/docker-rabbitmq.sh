#!/bin/bash

echo "--------------------------------------------------------------"
echo "starting rabbitmq broker..."

DOCKERIZE_VERSION=v0.6.1

wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz && sudo tar -C /usr/local/bin -xzvf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz && rm dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz

if [ -z $1 ]; then
    CUSTOM_CONF_MOUNT=""
else
    CUSTOM_CONF_MOUNT="-v $(realpath $1):/bitnami/rabbitmq/conf/custom.conf:ro"
fi

set -x
mkdir ./broker_logs
docker run -i --rm \
    -p 5672:5672 \
    -p 15672:15672 \
     --env RABBITMQ_USERNAME=guest \
     --env RABBITMQ_PASSWORD=guest \
    $CUSTOM_CONF_MOUNT \
    --mount type=bind,source=$(realpath ./broker_logs),target=/opt/bitnami/rabbitmq/var/log/rabbitmq/
    bitnami/rabbitmq:latest \
    >> broker.out 2>&1 &
dockerize -wait tcp://localhost:5672 -timeout 10m

echo "--------------------------------------------------------------"
echo "waiting for rabbitmq broker..."
sleep 15