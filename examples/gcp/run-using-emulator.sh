#!/bin/bash
# see https://cloud.google.com/pubsub/docs/emulator#using_the_emulator
gcloud beta emulators pubsub start --project=abc123 &
export PUBSUB_EMULATOR_HOST=localhost:8085
python publisher.py abc123 create top456
python subscriber.py abc123 create top456 sub789
python publisher.py abc123 publish top456
python subscriber.py abc123 receive sub789 5.0