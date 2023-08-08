S3Path
======

.. image:: https://badgen.net/pypi/v/s3path
    :target: https://pypi.org/project/s3path/
    :alt: Latest version

.. image:: https://github.com/liormizr/s3path/actions/workflows/testing.yml/badge.svg?branch=master&event=push
    :target: https://github.com/liormizr/s3path/actions/workflows/testing.yml
    :alt: S3Path CI

S3Path provide a Python convenient File-System/Path like interface for AWS S3 Service using boto3 S3 resource as a driver.

Like pathlib, but for S3 Buckets
________________________________

AWS S3 is among the most popular cloud storage solutions. It's object storage, is built to store and retrieve various amounts of data from anywhere.

Currently, Python developers use Boto3 as the default API to connect / put / get / list / delete files from S3.

S3Path blends Boto3's ease of use and the familiarity of pathlib api.

Install:
========

From PyPI:

.. code:: bash

    $ pip install s3path

From Conda:

.. code:: bash

    $ conda install -c conda-forge s3path

Basic use:
==========

The following example assumes an s3 bucket setup as specified bellow:

.. code:: bash

    $ aws s3 ls s3://pypi-proxy/

    2018-04-24 22:59:59        186 requests/index.html
    2018-04-24 22:59:57     485015 requests/requests-2.9.1.tar.gz
    2018-04-24 22:35:01      89112 boto3/boto3-1.4.1.tar.gz
    2018-04-24 22:35:02        180 boto3/index.html
    2018-04-24 22:35:19    3308919 botocore/botocore-1.4.93.tar.gz
    2018-04-24 22:35:36        188 botocore/index.html

Importing the main class:

.. code:: python

   >>> from s3path import S3Path

Listing "subdirectories" - s3 keys can be split like file-system with a `/` in s3path we:

.. code:: python

   >>> bucket_path = S3Path('/pypi-proxy/')
   >>> [path for path in bucket_path.iterdir() if path.is_dir()]
   [S3Path('/pypi-proxy/requests/'),
    S3Path('/pypi-proxy/boto3/'),
    S3Path('/pypi-proxy/botocore/')]

Listing html source files in this "directory" tree:

.. code:: python

   >>> bucket_path = S3Path('/pypi-proxy/')
   >>> list(bucket_path.glob('**/*.html'))
   [S3Path('/pypi-proxy/requests/index.html'),
    S3Path('/pypi-proxy/boto3/index.html'),
    S3Path('/pypi-proxy/botocore/index.html')]

Navigating inside a "directory" tree:

.. code:: python

   >>> bucket_path = S3Path('/pypi-proxy/')
   >>> boto3_package_path = bucket_path / 'boto3' / 'boto3-1.4.1.tar.gz'
   >>> boto3_package_path
   S3Path('/pypi-proxy/boto3/boto3-1.4.1.tar.gz')

Querying path properties:

.. code:: python

   >>> boto3_package_path = S3Path('/pypi-proxy/boto3/boto3-1.4.1.tar.gz')
   >>> boto3_package_path.exists()
   True
   >>> boto3_package_path.is_dir()
   False
   >>> boto3_package_path.is_file()
   True

Opening a "file" (s3 key):

.. code:: python

   >>> botocore_index_path = S3Path('/pypi-proxy/botocore/index.html')
   >>> with botocore_index_path.open() as f:
   >>>     print(f.read())
   """
   <!DOCTYPE html>
   <html>
   <head>
       <meta charset="UTF-8">
       <title>Package Index</title>
   </head>
   <body>
       <a href="botocore-1.4.93.tar.gz">botocore-1.4.93.tar.gz</a><br>
   </body>
   </html>
   """


Or Simply reading:

.. code:: python

   >>> botocore_index_path = S3Path('/pypi-proxy/botocore/index.html')
   >>> botocore_index_path.read_text()
   """
   <!DOCTYPE html>
   <html>
   <head>
       <meta charset="UTF-8">
       <title>Package Index</title>
   </head>
   <body>
       <a href="botocore-1.4.93.tar.gz">botocore-1.4.93.tar.gz</a><br>
   </body>
   </html>
   """

Versioned S3 Objects:
=====================

s3path supports versioned objects for S3 buckets that have versioning enabled. ``VersionedS3Path`` is a subclass of ``S3Path`` that supports all of its features. The main difference is an additional required ``version_id`` keyword parameter in each of its constructor methods.

.. code:: python

   >>> from s3path import VersionedS3Path
   >>> bucket, key, version_id = 'my-bucket', 'my-key', 'my-version-id'
   >>> VersionedS3Path(f'/{bucket}/{key}', version_id=version_id)
   VersionedS3Path('/my-bucket/my-key', version_id='my-version-id')
   >>> VersionedS3Path.from_uri(f's3://{bucket}/{key}', version_id=version_id)
   VersionedS3Path('/my-bucket/my-key', version_id='my-version-id')
   >>> VersionedS3Path.from_bucket_key(bucket=bucket, key=key, version_id=version_id)
   VersionedS3Path('/my-bucket/my-key', version_id='my-version-id')

New in version 0.5.0

Requirements:
=============

* Python >= 3.4
* boto3
* smart-open

Further Documentation:
======================

* `Advanced S3Path configuration`_ (S3 parameters, S3-compatible storage, etc.)
* `Abstract pathlib interface`_ implemented by S3Path
* `Boto3 vs S3Path usage examples`_


.. _Abstract pathlib interface: https://github.com/liormizr/s3path/blob/master/docs/interface.rst
.. _Boto3 vs S3Path usage examples: https://github.com/liormizr/s3path/blob/master/docs/comparison.rst
.. _Advanced S3Path configuration: https://github.com/liormizr/s3path/blob/master/docs/advance.rst
