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


S3 Compatible Storage:
----------------------

There are some cases that we want to use s3path for S3-Compatible Storage.

Some examples for S3-Compatible Storage can be:

* `LocalStack`_ - A fully functional local AWS cloud stack
* `MinIO`_ - MinIO is a High Performance Object Storage released under Apache License v2.0

`boto3`_ can be used as a SDK for such scenarios.

Therefor you can use s3path for them as well.

And even specify per "Bucket" what is the source.

This example show how to specify default AWS S3 parameters, a `LocalStack`_ Bucket, and a `MinIO`_ Bucket:

.. code:: python

   >>> import boto3
   >>> from botocore.client import Config
   >>> from s3path import PureS3Path, register_configuration_parameter
   >>> # Define path's for configuration
   >>> default_aws_s3_path = PureS3Path('/')
   >>> local_stack_bucket_path = PureS3Path('/LocalStackBucket/')
   >>> minio_bucket_path = PureS3Path('/MinIOBucket/')
   >>> # Define boto3 s3 resources
   >>> local_stack_resource = boto3.resource('s3', endpoint_url='http://localhost:4566')
   >>> minio_resource = boto3.resource(
       's3',
       endpoint_url='http://localhost:9000',
       aws_access_key_id='minio',
       aws_secret_access_key='minio123',
       config=Config(signature_version='s3v4'),
       region_name='us-east-1')
   >>> # Configure and map root path's per boto3 parameters or resources
   >>> register_configuration_parameter(default_aws_s3_path, parameters={'ServerSideEncryption': 'AES256'})
   >>> register_configuration_parameter(local_stack_bucket_path, resource=local_stack_resource)
   >>> register_configuration_parameter(minio_bucket_path, resource=minio_resource)


while using rename to/from S3 path we are using upload/download methods
but s3 transfer configuration set in your `.aws/config` file won't be honored
as this is not part of boto3_ but config interpretation is part of `aws-cli`_ in the
`RuntimeConfig` class.

So we provide a way to configurer additional params in order to be able to
configure TransferConfig and Callback:

.. code:: python

   >>> import sys
   >>> import threading
   >>> import boto3
   >>> from botocore.client import Config
   >>> from boto3.s3.transfer import TransferConfig
   >>> from s3path import PureS3Path, register_configuration_parameter
   >>> 
   >>> 
   >>> KB = 1024
   >>> MB = 1024 ** 2
   >>> GB = 1024 ** 3
   >>> 
   >>> 
   >>> class ProgressPercentage(object):
   >>> 
   >>>     def __init__(self, filename):
   >>>         self._filename = filename
   >>>         self._size = filename.stat().st_size
   >>>         self._seen_so_far = 0
   >>>         self._lock = threading.Lock()
   >>> 
   >>>     def __call__(self, bytes_amount):
   >>>         # To simplify we'll assume this is hooked up
   >>>         # to a single filename.
   >>>         with self._lock:
   >>>             self._seen_so_far += bytes_amount
   >>>             percentage = (self._seen_so_far / self._size) * 100
   >>>             sys.stdout.write(
   >>>                 "\r%s  %.3f GB / %.3f GB  (%.2f%%)" % (
   >>>                     self._filename, self._seen_so_far / GB, self._size / GB,
   >>>                     percentage))
   >>>             sys.stdout.flush()
   >>>
   >>> # Define path's for configuration
   >>> default_s3_path = PureS3Path('/')
   >>> # Define boto3 s3 resources
   >>> minio_resource = boto3.resource(
       's3',
       endpoint_url='http://localhost:9000',
       aws_access_key_id='minio',
       aws_secret_access_key='minio123',
       config=Config(signature_version='s3v4'),
       region_name='us-east-1')
   >>> register_configuration_parameter(
       default_s3_path,
       resource=minio_resource, 
       parameters={
         "StorageClass": "GLACIER",
         "transfert_config": TransferConfig(
            use_threads=True,
            multipart_threshold=30 * MB,
            multipart_chunksize=20 * MB,
            max_concurrency=10,
            max_bandwidth=None,
         ),
         "callback_class": ProgressPercentage,
       }, )


Please follow the following link to find more informations regarding TransferConfig_ parameters.
  

.. _aws-cli : https://github.com/aws/aws-cli/blob/master/awscli/customizations/s3/transferconfig.py
.. _TransferConfig: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/s3.html#boto3.s3.transfer.TransferConfig
.. _pathlib : https://docs.python.org/3/library/pathlib.html
.. _boto3 : https://github.com/boto/boto3
.. _configuration: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html
.. _profiles: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#shared-credentials-file
.. _setup_default_session: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/boto3.html?highlight=setup_default_session#boto3.setup_default_session
.. _s3-resource: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#service-resource
.. _LocalStack: https://github.com/localstack/localstack
.. _MinIO: https://docs.min.io/
