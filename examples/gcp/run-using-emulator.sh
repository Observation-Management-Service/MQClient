#!/bin/bash
# see https://cloud.google.com/pubsub/docs/emulator#using_the_emulator
num=`echo $((1 + $RANDOM % 1000))`
echo $num
sleep 2
gcloud beta emulators pubsub start --project="abc${num}" &
export PUBSUB_EMULATOR_HOST=localhost:8085
sleep 2
python publisher.py "abc${num}" create "top${num}"
sleep 10
echo "another 10..."
sleep 10
# python subscriber.py "abc${num}" create "top${num}" "sub${num}"
# sleep 2
python publisher.py "abc${num}" publish "top${num}"
sleep 200
# python subscriber.py "abc${num}" receive "sub${num}" 5.0