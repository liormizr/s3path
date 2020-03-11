# GCSPath

[![Build status](https://travis-ci.org/justindujardin/gcspath.svg?branch=master)](https://travis-ci.org/justindujardin/gcspath)
[![Pypi version](https://badgen.net/pypi/v/gcspath)](https://pypi.org/project/gcspath/)

> IMPORTANT: this library is not ready for use

GCSPath provides a convenient Pythonic File-System/Path like interface to Google Cloud Storage using [google-cloud-storage](https://pypi.org/project/google-cloud-storage/) package as a driver.

Like pathlib, but for GCS Buckets

---

GCS is among the popular cloud storage solutions. It's object storage built to store and retrieve various amounts of data from anywhere.

Rather than directly use `google-cloud-storage` to connect / put / get / list / delete files from GCS, gcspath extends pathlib classes to provide a familiar API for developers that normally work with local file paths.

# Install:

From PyPI:

```bash
$ pip install gcspath
```

From Conda:

```bash
$ conda install -c conda-forge gcspath
```

# Basic use:

The following example assumes an s3 bucket setup as specified bellow:

```bash
$ aws s3 ls s3://pypi-proxy/

2018-04-24 22:59:59        186 requests/index.html
2018-04-24 22:59:57     485015 requests/requests-2.9.1.tar.gz
2018-04-24 22:35:01      89112 boto3/boto3-1.4.1.tar.gz
2018-04-24 22:35:02        180 boto3/index.html
2018-04-24 22:35:19    3308919 botocore/botocore-1.4.93.tar.gz
2018-04-24 22:35:36        188 botocore/index.html
```

Importing the main class:

```python

   >>> from gcspath import GCSPath
```

Listing "subdirectories" - s3 keys can be split like file-system with a `/` in gcspath we:

```python

>>> bucket_path = GCSPath('/pypi-proxy/')
>>> [path for path in bucket_path.iterdir() if path.is_dir()]
[GCSPath('/pypi-proxy/requests/'),
GCSPath('/pypi-proxy/boto3/'),
GCSPath('/pypi-proxy/botocore/')]
```

Listing html source files in this "directory" tree:

```python

>>> bucket_path = GCSPath('/pypi-proxy/')
>>> list(bucket_path.glob('**/*.html'))
[GCSPath('/pypi-proxy/requests/index.html'),
GCSPath('/pypi-proxy/boto3/index.html'),
GCSPath('/pypi-proxy/botocore/index.html')]
```

Navigating inside a "directory" tree:

```python

>>> bucket_path = GCSPath('/pypi-proxy/')
>>> boto3_package_path = bucket_path / 'boto3' / 'boto3-1.4.1.tar.gz'
>>> boto3_package_path
GCSPath('/pypi-proxy/boto3/boto3-1.4.1.tar.gz')
```

Querying path properties:

```python

>>> boto3_package_path = GCSPath('/pypi-proxy/boto3/boto3-1.4.1.tar.gz')
>>> boto3_package_path.exists()
True
>>> boto3_package_path.is_dir()
False
>>> boto3_package_path.is_file()
True
```

Opening a "file" (s3 key):

```python

>>> botocore_index_path = GCSPath('/pypi-proxy/botocore/index.html')
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
```

Or Simply reading:

```python

>>> botocore_index_path = GCSPath('/pypi-proxy/botocore/index.html')
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
```

# Requirements:

- Python >= 3.6
- google-cloud-storage
