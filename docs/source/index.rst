S3Path 
======
________________________________
Like pathlib, but for S3 Buckets
________________________________

AWS S3 is among the most popular cloud storage solutions. It's object storage, is built to store and retrieve various amounts of data from anywhere.

Currently, Python developers use Boto3 as the default API to connect / put / get / list / delete files from S3.

S3Path blends Boto3's ease of use and the farmiliarity or pathlib's api.

Basic use:
==========

The following example assumes an s3 bucket setup as specified bellow:

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
