#!/usr/bin/env python
"""Setup."""

import os
import subprocess

from setuptools import setup  # type: ignore[import]

subprocess.run(
    "pip install git+https://github.com/WIPACrepo/wipac-dev-tools.git".split(),
    check=True,
)
from wipac_dev_tools import SetupShop  # noqa: E402  # pylint: disable=C0413

shop = SetupShop(
    "mqclient",
    os.path.abspath(os.path.dirname(__file__)),
    ((3, 6), (3, 9)),
    "Message Queue Client Base Abstraction",
)

# FIXME - remove this hacky code
kwargs = shop.get_kwargs()
kwargs["install_requires"].append(
    "wipac_telemetry @ git+https://github.com/WIPACrepo//wipac-telemetry-prototype@allow-bytes-as-attrs"
)
setup(
    url="https://github.com/WIPACrepo/MQClient",
    package_data={shop.name: ["py.typed", "requirements.txt"]},
    **shop.get_kwargs(subpackages=["testing"]),
)
