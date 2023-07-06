"""Public init."""

import os

from .queue import Queue

__all__ = ["Queue"]


for envvar in ["RABBITMQ_HEARTBEAT", "PULSAR_UNACKED_MESSAGES_TIMEOUT_SEC"]:
    if os.getenv(envvar):
        RuntimeError(f"Environment variable {envvar} has been deprecated.")


# version is a human-readable version number.

# version_info is a four-tuple for programmatic comparison. The first
# three numbers are the components of the version number. The fourth
# is zero for an official release, positive for a development branch,
# or negative for a release candidate or beta (after the base version
# number has been incremented)
__version__ = "2.1.0"
version_info = (
    int(__version__.split(".")[0]),
    int(__version__.split(".")[1]),
    int(__version__.split(".")[2]),
    0,
)
