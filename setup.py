#!/usr/bin/env python
from distutils.core import setup
from setuptools import find_packages

with open("README.rst", "r") as fh:
    long_description = fh.read()

setup(
    name='s3path',
    version='0.0.1',
    url='https://github.com/liormizr/s3path',
    author='Lior Mizrahi',
    author_email='li.mizr@gmail.com',
    packages=find_packages(),
    install_requires=[
        'boto3',
    ],
    licence='Apache 2.0',
    long_description=long_description,
    long_description_content_type="text/markdown",
    python_requires='>= 3.4',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
)
