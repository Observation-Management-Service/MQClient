#!/usr/bin/env python
"""Setup."""


import os

import setuptools  # type: ignore[import]

kwargs = {}

current_path = os.path.dirname(os.path.realpath(__file__))

with open(os.path.join(current_path, "MQClient", "__init__.py")) as f:
    for line in f.readlines():
        if "__version__" in line:
            # grab "X.Y.Z" from "__version__ = 'X.Y.Z'" (quote-style insensitive)
            kwargs["version"] = line.replace('"', "'").split("=")[-1].split("'")[1]
            break
    else:
        raise Exception("cannot find __version__")

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="MQClient",
    author="IceCube Developers",
    author_email="developers@icecube.wisc.edu",
    description="Message queue client abstraction",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/WIPACrepo/MQClient",
    packages=setuptools.find_packages(),
    package_data={"MQClient": ["py.typed"]},
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
    extras_require={
        "RabbitMQ": ["pika"],
        "tests": [
            "pytest",
            "pytest-asyncio",
            "pytest-flake8",
            "pytest-mypy",
            "pytest-mock",
        ],
    },
    **kwargs,
)
