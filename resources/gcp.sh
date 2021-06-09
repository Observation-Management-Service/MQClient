#!/bin/bash
docker run --rm -it \
    --name gcloud-config \
    google/cloud-sdk gcloud auth login
