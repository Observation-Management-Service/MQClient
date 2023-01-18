"""Configuration constants."""

import dataclasses as dc

from wipac_dev_tools import from_environment_as_dataclass

# pylint:disable=invalid-name

#
# Env var constants: set as constants & typecast
#


@dc.dataclass(frozen=True)
class EnvConfig:
    """For storing environment variables, typed."""

    EWMS_MQ_ADDRESS: str = "localhost"
    EWMS_MQ_PREFETCH: int = 1
    EWMS_MQ_TIMEOUT: int = 1 * 60
    EWMS_MQ_AUTH_TOKEN: str = ""


ENV = from_environment_as_dataclass(EnvConfig)
