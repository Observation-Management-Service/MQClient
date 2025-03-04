#!/bin/bash
set -euo pipefail
set -e

if [ -z $1 ]; then
    echo "MISSING ARG: message_compression_benchmarking.sh N_ITERATIONS"
    exit 1
fi
N_ITERATIONS=$1

all_algos='bz2 lzma zstd gzip lz4'
all_data='large_dict medium_string skyscan_i3_frame_pkl'

for algo in $all_algos; do
	echo
	echo "=============================================================="
	echo "$algo:"
	for data in $all_data; do
		echo
		echo "--------------------------------------"
		echo "data=$data"
		echo
		python3 message_compression_benchmarking.py --algo $algo --data $data --only-size
		echo
		echo "iterations=$N_ITERATIONS (compress + decompress)"
		time for i in $(seq 1 $N_ITERATIONS); do python3 message_compression_benchmarking.py --algo $algo --data $data; done
	done
done
