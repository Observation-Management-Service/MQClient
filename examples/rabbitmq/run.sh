#!/bin/bash
python examples/rabbitmq/worker.py &
python examples/rabbitmq/server.py