S3Path VS Boto3 S3 SDK
======================

Most of the boto3 examples are taken from here: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-examples.html

Buckets List:
-------------

S3Path Example:

.. code:: python

   >>> from s3path import S3Path
   >>> for bucket in S3Path('/').iterdir():
   ...     print(bucket)

boto3 Example:

.. code:: python

   >>> import boto3
   >>> # Create an S3 client
   >>> s3 = boto3.client('s3')
   >>> # Call S3 to list current buckets
   >>> response = s3.list_buckets()
   >>> # Get a list of all bucket names from the response
   >>> buckets = [bucket['Name'] for bucket in response['Buckets']]
   >>> # Print out the bucket list
   >>> for bucket in buckets:
   ...     print(bucket)

Create an Amazon S3 Bucket
--------------------------

S3Path Example:

.. code:: python

   >>> from s3path import S3Path
   >>> S3Path('/my-bucket/').mkdir()

boto3 Example:

.. code:: python

   >>> import boto3
   >>> s3 = boto3.resource('s3')
   >>> s3.create_bucket(Bucket='my-bucket')

Upload a File to an Amazon S3 Bucket
------------------------------------

S3Path Example:

.. code:: python

   >>> from pathlib import Path
   >>> from s3path import S3Path
   >>> local_path = Path('/tmp/hello.txt')
   >>> S3Path('/my-bucket/hello.txt').write_text(local_path.read_text())

S3Path Example (buffered, to avoid loading large files into memory):

.. code:: python

   >>> import shutil
   >>> from pathlib import Path
   >>> from s3path import S3Path
   >>> local_path = Path('/tmp/hello.txt')
   >>> remote_path = S3Path('/my-bucket/hello.txt')
   >>> with local_path.open('rb') as src, remote_path.open('wb') as dst:
   >>>     shutil.copyfileobj(src, dst)

boto3 Example:

.. code:: python

   >>> import boto3
   >>> s3 = boto3.resource('s3')
   >>> bucket = s3.Bucket('my-bucket')
   >>> bucket.upload_file(Fileobj='/tmp/hello.txt', Key='hello.txt')

Downloading a File
------------------

S3Path Example:

.. code:: python

   >>> from pathlib import Path
   >>> from s3path import S3Path
   >>> local_path = Path('./my_local_image.jpg')
   >>> local_path.write_text(S3Path('/my-bucket/my_image_in_s3.jpg').read_text())

boto3 Example:

.. code:: python

   >>> import boto3
   >>> import botocore
   >>> s3 = boto3.resource('s3')
   >>>
   >>> try:
   >>>     bucket = s3.Bucket('my-bucket')
   >>>     bucket.download_file(Key='my_image_in_s3.jpg', Filename='my_local_image.jpg')
   >>> except botocore.exceptions.ClientError as e:
   >>>     if e.response['Error']['Code'] == "404":
   >>>         print("The object does not exist.")
   >>>     else:
   >>>         raise

Retrieving subfolders names in S3 bucket
----------------------------------------

S3Path Example:

.. code:: python

   >>> from s3path import S3Path
   >>> for path in S3Path('/my-bucket/prefix-name-with-slash/').iterdir():
   >>>     if path.is_dir():
   >>>         print('sub folder : ', path)

boto3 Example:

.. code:: python

   >>> import boto3
   >>> s3_client = boto3.client('s3')
   >>> result = client.list_objects(Bucket='my-bucket', Prefix='prefix-name-with-slash/', Delimiter='/')
   >>> for o in result.get('CommonPrefixes'):
   >>>     print('sub folder : ', o.get('Prefix'))
