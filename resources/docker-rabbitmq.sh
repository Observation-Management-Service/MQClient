#!/bin/bash
set -euo pipefail

echo "--------------------------------------------------------------"
echo "starting rabbitmq broker..."

if [ -z "${1-}" ]; then
    echo "MISSING ARG: docker-rabbitmq.sh CONTAINER_NAME [CUSTOM_CONF_FILEPATH]"
    exit 1
fi

DOCKERIZE_VERSION=v0.6.1

wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz && sudo tar -C /usr/local/bin -xzvf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz && rm dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz

if [ -z "${2-}" ]; then
    echo -e "log.console.level = debug\n" >>"./rabbitmq-custom.conf"
    echo -e "loopback_users = none\n" >>"./rabbitmq-custom.conf" # allows guest/guest from non-localhost
    CUSTOM_CONF_MOUNT="-v $(realpath './rabbitmq-custom.conf'):/bitnami/rabbitmq/conf/custom.conf:ro"
else
    CUSTOM_CONF_MOUNT="-v $(realpath $2):/bitnami/rabbitmq/conf/custom.conf:ro"
fi

set -x
mkdir ./broker_logs
docker run -i --rm --name $1 \
    --network=host \
    --env RABBITMQ_USERNAME=guest \
    --env RABBITMQ_PASSWORD=guest \
    --env BITNAMI_DEBUG=true \
    $CUSTOM_CONF_MOUNT \
    --mount type=bind,source=$(realpath ./broker_logs),target=/opt/bitnami/rabbitmq/var/log/rabbitmq/ \
    bitnami/rabbitmq:latest \
    >>broker.out 2>&1 &
dockerize -wait tcp://localhost:5672 -timeout 1m
dockerize -wait tcp://localhost:15672 -timeout 1m

echo "--------------------------------------------------------------"
echo "waiting for rabbitmq broker..."
sleep 15
