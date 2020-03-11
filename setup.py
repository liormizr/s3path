#!/usr/bin/env python
import gcspath
from setuptools import setup

with open("README.rst", "r") as fh:
    long_description = fh.read()

setup(
    name=gcspath.__name__,
    version=gcspath.__version__,
    url="https://github.com/justindujardin/gcspath",
    author="Justin DuJardin",
    author_email="justin@explosion.ai",
    py_modules=["gcspath"],
    install_requires=["google-cloud-storage"],
    license="Apache 2.0",
    long_description=long_description,
    long_description_content_type="text/x-rst",
    python_requires=">= 3.6",
    include_package_data=True,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
)
