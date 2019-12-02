import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="MQClient",
    version="0.0.1",
    author="IceCube Developers",
    author_email="developers@icecube.wisc.edu",
    description="Message queue client abstraction",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/WIPACrepo/MQClient",
    packages=setuptools.find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
    extras_require={
        'RabbitMQ': ['pika'],
    }
)