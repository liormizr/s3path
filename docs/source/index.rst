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

   PureS3Path objects implement the :class:`os.PathLike` interface, allowing them
   to be used anywhere the interface is accepted.

   .. versionchanged:: 3.6
      Added support for the :class:`os.PathLike` interface.


Operators
^^^^^^^^^

The slash operator helps create child paths, similarly to :func:`os.path.join`::

   >>> p = PureS3Path('/etc')
   >>> p
   PureS3Path('/etc')
   >>> p / 'init.d' / 'apache2'
   PureS3Path('/etc/init.d/apache2')
   >>> q = PureS3Path('bin')
   >>> '/usr' / q
   PureS3Path('/usr/bin')


Accessing individual parts
^^^^^^^^^^^^^^^^^^^^^^^^^^

To access the individual "parts" (components) of a path, use the following
property:

.. data:: PureS3Path.parts

   A tuple giving access to the path's various components::

      >>> p = PureS3Path('foo//bar')
      >>> p.parts
      ('foo', 'bar')

      >>> p = PureS3Path('/foo/bar')('c:/Program Files/PSF')
      >>> p.parts
      ('/', 'foo', 'bar')



Methods and properties
^^^^^^^^^^^^^^^^^^^^^^

.. testsetup::

   from pathlib import PurePosixPath, PureWindowsPath

 PureS3Path objects modify following methods and properties:

.. data:: PureS3Path.drive

   The drive property will simply return an empty string::

      >>> PureS3Path('foo//bar').drive
      ''


.. data:: PurePath.root

   A string representing the (local or global) root. This method will return an empty string or '/'::

      >>> PureS3Path('foo//bar').root
      ''
      >>> PureS3Path('../bar').root
      ''
      >>> PureS3Path('/foo/bar').root
      '/'

   UNC shares always have a root::

 
.. data:: PurePath.anchor

   Modified to return an empty string or '/'::

      >>> PureS3Path('foo//bar').anchor
      ''
      >>> PureS3Path('/foo/bar').anchor
      '/'


.. data:: PureS3Path.parents

   An immutable sequence providing access to the logical ancestors of
   the path::


.. data:: PurePath.parent

   The logical parent of the path::

      >>> p = PurePosixPath('/a/b/c/d')
      >>> p.parent
      PurePosixPath('/a/b/c')

   You cannot go past an anchor, or empty path::

      >>> p = PureS3Path('foo//bar').parent 
      >>> p.parent
      PureS3Path('foo')
      >>> p = PureS3Path('foo/../bar')
      >>> p.parent
      PureS3Path('.')

   .. note::
      This is a purely lexical operation, hence the following behaviour::

         >>> p = PureS3Path('../bar')
         >>> p.parent
         PureS3Path('foo', '../bar')

      If you want to walk an arbitrary filesystem path upwards, it is
      recommended to first call :meth:`Path.resolve` so as to resolve
      symlinks and eliminate `".."` components.


.. data:: PureS3Path.name

   A string representing the final path component, excluding the drive and
   root, if any::

      >>> PureS3Path('my/library/setup.py').name
      'setup.py'



.. data:: PureS3Path.suffix

   The file extension of the final component, if any::

      >>> PureS3Path('my/library/setup.py').suffix
      '.py'
      >>> PureS3Path('my/library.tar.gz').suffix
      '.gz'
      >>> PureS3Path('my/library').suffix
      ''


.. data:: PurePath.suffixes

   A list of the path's file extensions::

      >>> PureS3Path('my/library.tar.gar').suffixes
      ['.tar', '.gar']
      >>> PureS3Path('my/library.tar.gz').suffixes
      ['.tar', '.gz']
      >>> PureS3Path('my/library').suffixes
      []


.. data:: PurePath.stem

   The final path component, without its suffix::

      >>> PureS3Path('my/library.tar.gz').stem
      'library.tar'
      >>> PureS3Path('my/library.tar').stem
      'library'
      >>> PureS3Path('my/library').stem
      'library'


.. method:: PurePath.as_posix()

   Return a string representation of the path with forward slashes (``/``)::

      >>> p = PureWindowsPath('c:\\windows')
      >>> str(p)
      'c:\\windows'
      >>> p.as_posix()
      'c:/windows'


.. method:: PurePath.as_uri()

   Represent the path as a ``file`` URI.  :exc:`ValueError` is raised if
   the path isn't absolute.

      >>> p = PurePosixPath('/etc/passwd')
      >>> p.as_uri()
      'file:///etc/passwd'
      >>> p = PureWindowsPath('c:/Windows')
      >>> p.as_uri()
      'file:///c:/Windows'


.. method:: PurePath.is_absolute()

   Return whether the path is absolute or not.  A path is considered absolute
   if it has both a root and (if the flavour allows) a drive::

      >>> PurePosixPath('/a/b').is_absolute()
      True
      >>> PurePosixPath('a/b').is_absolute()
      False

      >>> PureWindowsPath('c:/a/b').is_absolute()
      True
      >>> PureWindowsPath('/a/b').is_absolute()
      False
      >>> PureWindowsPath('c:').is_absolute()
      False
      >>> PureWindowsPath('//some/share').is_absolute()
      True


.. method:: PurePath.is_reserved()

   With :class:`PureWindowsPath`, return ``True`` if the path is considered
   reserved under Windows, ``False`` otherwise.  With :class:`PurePosixPath`,
   ``False`` is always returned.

      >>> PureWindowsPath('nul').is_reserved()
      True
      >>> PurePosixPath('nul').is_reserved()
      False

   File system calls on reserved paths can fail mysteriously or have
   unintended effects.


.. method:: PurePath.joinpath(*other)

   Calling this method is equivalent to combining the path with each of
   the *other* arguments in turn::

      >>> PurePosixPath('/etc').joinpath('passwd')
      PurePosixPath('/etc/passwd')
      >>> PurePosixPath('/etc').joinpath(PurePosixPath('passwd'))
      PurePosixPath('/etc/passwd')
      >>> PurePosixPath('/etc').joinpath('init.d', 'apache2')
      PurePosixPath('/etc/init.d/apache2')
      >>> PureWindowsPath('c:').joinpath('/Program Files')
      PureWindowsPath('c:/Program Files')


.. method:: PurePath.match(pattern)

   Match this path against the provided glob-style pattern.  Return ``True``
   if matching is successful, ``False`` otherwise.

   If *pattern* is relative, the path can be either relative or absolute,
   and matching is done from the right::

      >>> PurePath('a/b.py').match('*.py')
      True
      >>> PurePath('/a/b/c.py').match('b/*.py')
      True
      >>> PurePath('/a/b/c.py').match('a/*.py')
      False

   If *pattern* is absolute, the path must be absolute, and the whole path
   must match::

      >>> PurePath('/a.py').match('/*.py')
      True
      >>> PurePath('a/b.py').match('/*.py')
      False

   As with other methods, case-sensitivity is observed::

      >>> PureWindowsPath('b.py').match('*.PY')
      True


.. method:: PurePath.relative_to(*other)

   Compute a version of this path relative to the path represented by
   *other*.  If it's impossible, ValueError is raised::

      >>> p = PurePosixPath('/etc/passwd')
      >>> p.relative_to('/')
      PurePosixPath('etc/passwd')
      >>> p.relative_to('/etc')
      PurePosixPath('passwd')
      >>> p.relative_to('/usr')
      Traceback (most recent call last):
        File "<stdin>", line 1, in <module>
        File "pathlib.py", line 694, in relative_to
          .format(str(self), str(formatted)))
      ValueError: '/etc/passwd' does not start with '/usr'


.. method:: PurePath.with_name(name)

   Return a new path with the :attr:`name` changed.  If the original path
   doesn't have a name, ValueError is raised::

      >>> p = PureWindowsPath('c:/Downloads/pathlib.tar.gz')
      >>> p.with_name('setup.py')
      PureWindowsPath('c:/Downloads/setup.py')
      >>> p = PureWindowsPath('c:/')
      >>> p.with_name('setup.py')
      Traceback (most recent call last):
        File "<stdin>", line 1, in <module>
        File "/home/antoine/cpython/default/Lib/pathlib.py", line 751, in with_name
          raise ValueError("%r has an empty name" % (self,))
      ValueError: PureWindowsPath('c:/') has an empty name


.. method:: PurePath.with_suffix(suffix)

   Return a new path with the :attr:`suffix` changed.  If the original path
   doesn't have a suffix, the new *suffix* is appended instead.  If the
   *suffix* is an empty string, the original suffix is removed::

      >>> p = PureWindowsPath('c:/Downloads/pathlib.tar.gz')
      >>> p.with_suffix('.bz2')
      PureWindowsPath('c:/Downloads/pathlib.tar.bz2')
      >>> p = PureWindowsPath('README')
      >>> p.with_suffix('.txt')
      PureWindowsPath('README.txt')
      >>> p = PureWindowsPath('README.txt')
      >>> p.with_suffix('')
      PureWindowsPath('README')


.. _concrete-paths:




