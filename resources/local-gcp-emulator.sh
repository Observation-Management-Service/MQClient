#!/bin/bash

if [ "$PUBSUB_EMULATOR_HOST" != "localhost:8085" ]; then
    echo "Don't forget to 'export PUBSUB_EMULATOR_HOST=localhost:8085'"
    exit 1
fi

echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
sudo apt-get install apt-transport-https ca-certificates gnupg
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -
sudo apt-get update
sudo apt-get install google-cloud-sdk
sudo apt-get install google-cloud-sdk-pubsub-emulator
gcloud components list

gcloud beta emulators pubsub start --project="i3-gcp-proj" &