Advance features (configurations/s3 parameters):
================================================

Basically s3path is trying to be as pure as possible from any non `pathlib`_ features.

The goal is to take the AWS S3 service and integrate it into `pathlib`_'s interface without changes.

Only then s3path provides a Python-convenient File-System/Path like interface for AWS's S3 service using `boto3`_ S3 resource as a driver.


Configurations:
---------------

s3path uses `boto3`_ as the SDK for AWS S3 service.

To use `boto3`_ you first need to configure it. For the full documentation see `configuration`_.

`boto3`_ has multiple ways to input configurations, s3path only supportes the following:

1. Environment variables
#. Shared credential file (~/.aws/credentials)
#. AWS config file (~/.aws/config)
#. Assume Role provider
#. Instance metadata service on an Amazon EC2 instance that has an IAM role configured.

With s3path, you can't specify configurations. The only way to specify configurations in code, is with `setup_default_session`_.

For Example:

.. code:: python

   >>> import boto3
   >>> from s3path import S3Path
   >>> boto3.setup_default_session(
   ...     region_name='us-east-1',
   ...     aws_access_key_id='<access-key>',
   ...     aws_secret_access_key='<access-secret>')
   >>>
   >>> bucket_path = S3Path('/pypi-proxy/')
   >>> [path for path in bucket_path.iterdir() if path.is_dir()]
   ... [S3Path('/pypi-proxy/requests/'),
   ...  S3Path('/pypi-proxy/boto3/'),
   ...  S3Path('/pypi-proxy/botocore/')]

Parameters:
-----------

We can map any kind of parameters that `boto3`_ `s3-resource`_ methods supports per path.

For Example:

If you want to add Server-side encryption to your Bucket, you may do it per path like this:

.. code:: python

   >>> from s3path import S3Path, register_configuration_parameter
   >>> bucket = S3Path('/my-bucket/')
   >>> register_configuration_parameter(bucket, parameters={'ServerSideEncryption': 'AES256'})

This will work for every s3path.

S3Path('/') - parameters that will be used as default

S3Path('/bucket/') - parameters that will be used per bucket

S3Path('/bucket/key-prefix-directory/') - parameters that will be used per bucket, key prefix

**NOTE:** We recommend configuring everything only in one place and not in the code.

.. _pathlib : https://docs.python.org/3/library/pathlib.html
.. _boto3 : https://github.com/boto/boto3
.. _configuration: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html
.. _profiles: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#shared-credentials-file
.. _setup_default_session: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/boto3.html?highlight=setup_default_session#boto3.setup_default_session
.. _s3-resource: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#service-resource
