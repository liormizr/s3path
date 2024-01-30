#!/usr/bin/env python
from setuptools import setup

with open("README.rst", "r") as fh:
    long_description = fh.read()
setup(
    name='s3path',
    version='0.5.2',
    url='https://github.com/liormizr/s3path',
    author='Lior Mizrahi',
    author_email='li.mizr@gmail.com',
    # py_modules=['s3path'],
    packages=['s3path'],
    install_requires=[
        'boto3>=1.16.35',
        'smart-open',
    ],
    license='Apache 2.0',
    long_description=long_description,
    long_description_content_type='text/x-rst',
    python_requires='>=3.8',
    include_package_data=True,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
    ],
)
