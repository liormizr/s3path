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


PureS3Path paths
----------

PureS3Path could be treated as a PurePath flavour. It treats S3 Buckets as a posix file system. The objects provide path-handling operations which don't actually
access a filesystem. 

     ::

      >>> PureS3Path('setup.py')('setup.py')      # instantiating PureS3Path
      PureS3Path('setup.py')

   Each element of *pathsegments* can be either a string representing a
   path segment, an object implementing the :class:`os.PathLike` interface
   which returns a string, or another path object::

      >>> PureS3Path('foo', 'some/path', 'bar')
      PurePosixPath('foo/some/path/bar')
      >>> PurePath(Path('foo'), Path('bar'))
      PurePosixPath('foo/bar')

   When *pathsegments* is empty, the current directory is assumed::

      >>> PureS3Path()
      PureS3Path('.')('.')

   When several absolute paths are given, the last is taken as an anchor
   (mimicking :func:`os.path.join`'s behaviour)::

      >>> PureS3Path('/etc', '/usr', 'lib64')


   

   Spurious slashes and single dots are collapsed.::

      >>> PureS3Path('foo//bar')
      PureS3Path('foo/bar')
      >>> PureS3Path('foo/./bar')
      PureS3Path('foo/bar')
      
   Double dots (``'..'``) are treated as follows. 
   This is different then PurePath since
   symbolic links ar not a concern::   
      >>> PureS3Path('foo/../bar')
      PureS3Path('bar')

   (a naïve approach would make ``PurePosixPath('foo/../bar')`` equivalent
   to ``PurePosixPath('bar')``, which is wrong if ``foo`` is a symbolic link
   to another directory)

   Pure path objects implement the :class:`os.PathLike` interface, allowing them
   to be used anywhere the interface is accepted.

   .. versionchanged:: 3.6
      Added support for the :class:`os.PathLike` interface.

.. class:: PurePosixPath(*pathsegments)

   A subclass of :class:`PurePath`, this path flavour represents non-Windows
   filesystem paths::

      >>> PurePosixPath('/etc')
      PurePosixPath('/etc')

   *pathsegments* is specified similarly to :class:`PurePath`.

.. class:: PureWindowsPath(*pathsegments)

   A subclass of :class:`PurePath`, this path flavour represents Windows
   filesystem paths::

      >>> PureWindowsPath('c:/Program Files/')
      PureWindowsPath('c:/Program Files')

   *pathsegments* is specified similarly to :class:`PurePath`.

Regardless of the system you're running on, you can instantiate all of
these classes, since they don't provide any operation that does system calls.

