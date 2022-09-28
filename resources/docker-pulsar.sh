#!/bin/bash

DOCKERIZE_VERSION=v0.6.1
PULSAR_CONTAINER=local-pulsar

wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz && sudo tar -C /usr/local/bin -xzvf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz && rm dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz

docker run -i -d --rm \
    -p 6650:6650 \
    -p 8080:8080 \
    --name $PULSAR_CONTAINER apachepulsar/pulsar:2.6.0 /bin/bash \
    -c "sed -i s/brokerDeleteInactiveTopicsEnabled=.*/brokerDeleteInactiveTopicsEnabled=false/ /pulsar/conf/standalone.conf && bin/pulsar standalone"
dockerize -wait tcp://localhost:8080 -timeout 10m
dockerize -wait tcp://localhost:6650 -timeout 10m