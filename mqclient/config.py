"""Configurations."""

import os

for envvar in ["RABBITMQ_HEARTBEAT", "PULSAR_UNACKED_MESSAGES_TIMEOUT_SEC"]:
    if os.getenv(envvar):
        RuntimeError(f"Environment variable {envvar} has been deprecated.")
