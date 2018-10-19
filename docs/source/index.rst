S3Path Simple Pathlib lick API for AWS S3 service:
==================================================

AWS S3 is one of the more popular services in AWS.
S3 is a object storage built to store and retrieve any amount of data from anywhere.

In python we have boto3 for the latest API to connect / put / get / list / delete files from s3.

This library is trying to integrate boto3 to pathlib api.

Basic use:
==========

For example if we have this s3 bucket setup:

* Bucket name = bucket
* Keys:
   * directory/Test.test
   * pathlib.py
   * setup.py
   * test_pathlib.py
   * build/lib/pathlib.py
   * docs/conf.py
   * docs/make.bat
   * docs/index.rst
   * docs/Makefile
   * docs/_templates/somefile.txt
   * docs/_build/conf.py
   * docs/_static/conf.py

Importing the main class::

   >>> from s3path import S3Path

Listing "subdirectories" - s3 keys can be splited like file-system with a `/` in s3path we ::

   >>> p = S3Path('/bucket/docs/')
   >>> [x for x in p.iterdir() if x.is_dir()]
   [S3Path('/bucket/docs/_templates'),
    S3Path('/bucket/docs/_build'),
    S3Path('/bucket/docs/_static')]

Listing Python source files in this "directory" tree::

   >>> list(p.glob('**/*.py'))
   [S3Path('/bucket/docs/conf.py'),
    S3Path('/bucket/docs/_build/conf.py'),
    S3Path('/bucket/docs/'),
    S3Path('/bucket/docs/docs/_static/conf.py')]

Navigating inside a "directory" tree::

   >>> p = Path('/bucket')
   >>> q = bucket_path / 'build' / 'lib' / 'pathlib.py'
   >>> q
   S3Path('/bucket/build/lib/pathlib.py')

Querying path properties::

   >>> q.exists()
   True
   >>> q.is_dir()
   False

Opening a "file" (s3 key)::

   >>> with q.open() as f: f.readline()
   ...
   '#!/bin/bash\n'

Guide
^^^^^


.. toctree::
   :maxdepth: 2
   :caption: Contents:



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
