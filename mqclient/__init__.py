"""Public init."""


from . import backend_interface, log_msgs
from .queue import Queue

__all__ = ["Queue", "backend_interface", "log_msgs"]

# version is a human-readable version number.

# version_info is a four-tuple for programmatic comparison. The first
# three numbers are the components of the version number. The fourth
# is zero for an official release, positive for a development branch,
# or negative for a release candidate or beta (after the base version
# number has been incremented)
__version__ = "0.0.7"
version_info = (
    int(__version__.split(".")[0]),
    int(__version__.split(".")[1]),
    int(__version__.split(".")[2]),
    0,
)