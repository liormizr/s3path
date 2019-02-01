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


.. data:: PureS3Path.suffixes

   A list of the path's file extensions::

      >>> PureS3Path('my/library.tar.gar').suffixes
      ['.tar', '.gar']
      >>> PureS3Path('my/library.tar.gz').suffixes
      ['.tar', '.gz']
      >>> PureS3Path('my/library').suffixes
      []


.. data:: PureS3Path.stem

   The final path component, without its suffix::

      >>> PureS3Path('my/library.tar.gz').stem
      'library.tar'
      >>> PureS3Path('my/library.tar').stem
      'library'
      >>> PureS3Path('my/library').stem
      'library'


.. method:: PureS3Path.as_posix()

   Return a string representation of the path with forward slashes (``/``)::

      >>> p = PureS3Path('/usr/bin')
      >>> str(p)
      '/usr/bin'
      >>> p.as_posix()
      '/usr/bin'


.. method:: PureS3Path.as_uri()

   Represent the path as a ``file`` URI.  :exc:`ValueError` is raised if
   the path isn't absolute.

      >>> p = PureS3Path('/etc/passwd')
      >>> p.as_uri()
      's3://etc/passwd'
      >>> p = PureS3Path('/bucket/key')
      >>> p.as_uri()
      's3://bucket/key'


.. method:: PureS3Path('/a/b').is_absolute()

   Return whether the path is absolute or not.  A path is considered absolute
   if it has both a root and (if the flavour allows) a drive::

      >>> PureS3Path('/a/b').is_absolute()
      True
      >>> PureS3Path('a/b').is_absolute()
      False


.. method:: PureS3Path.is_reserved()

   With :class:`PureS3Path`,
   ``False`` is always returned.

      >>> PureS3Path('a/b').is_reserved()
      False
      >>> PureS3Path('/a/b').is_reserved()
      False

   File system calls on reserved paths can fail mysteriously or have
   unintended effects.


.. method:: PurePath.joinpath(*other)

   Calling this method is equivalent to combining the path with each of
   the *other* arguments in turn::

      >>> PureS3Path('/etc').joinpath('passwd')
      PureS3Path('/etc/passwd')
      >>> PureS3Path('/etc').joinpath(PureS3Path('passwd'))
      PureS3Path('/etc/passwd')
      >>> PureS3Path('/etc').joinpath('init.d', 'apache2') 
      PureS3Path('/etc/init.d/apache2')

.. method:: PureS3Path.match(pattern)

   Match this path against the provided glob-style pattern.  Return ``True``
   if matching is successful, ``False`` otherwise.

   If *pattern* is relative, the path can be either relative or absolute,
   and matching is done from the right::

      >>> PureS3Path('a/b.py').match('*.py')
      True
      >>> PureS3Path('/a/b/c.py').match('b/*.py')
      True
      >>> PureS3Path('/a/b/c.py').match('a/*.py')
      False

   If *pattern* is absolute, the path must be absolute, and the whole path
   must match::

      >>> PureS3Path('/a.py').match('/*.py')
      True
      >>> PureS3Path('a/b.py').match('/*.py')
      False


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

      >>> p = PureS3Path('/Downloads/pathlib.tar.gz')
      >>> p.with_name('setup.py')  
      PureS3Path('/Downloads/setup.py')
      >>> p = PureS3Path('/')
      >>> p.with_name('setup.py')
      Traceback (most recent call last):
        File "<stdin>", line 1, in <module>
        ...
      ValueError: PureS3Path('/') has an empty name


.. method:: PurePath.with_suffix(suffix)

   Return a new path with the :attr:`suffix` changed.  If the original path
   doesn't have a suffix, the new *suffix* is appended instead.  If the
   *suffix* is an empty string, the original suffix is removed::

      >>> p = PureS3Path('/Downloads/pathlib.tar.gz')
      >>> p.with_suffix('.bz2')
      PureS3Path('/Downloads/pathlib.tar.bz2')
      >>> p = PureWindowsPath('README')
      >>> p.with_suffix('.txt')
      PureWindowsPath('README.txt')
      >>> p = PureS3Path('README')
      >>> p.with_suffix('')
      PureS3Path('README')


.. _concrete-paths:

Concrete paths
--------------

Concrete paths are subclasses of the pure path classes.  In addition to
operations provided by the latter, they also provide methods to do system
calls on path objects.  There are three ways to instantiate concrete paths:

.. class:: S3Path(*pathsegments)

   A subclass of :class:`PureS3Path`, this class represents concrete paths of
   the system's path flavour (instantiating it creates either a
   :class:`S3Path` or a :class:`WindowsPath`)::

      >>> Path('setup.py')
      PosixPath('setup.py')

   *pathsegments* is specified similarly to :class:`PurePath`.

.. class:: PosixPath(*pathsegments)

   A subclass of :class:`Path` and :class:`PurePosixPath`, this class
   represents concrete non-Windows filesystem paths::

      >>> PosixPath('/etc')
      PosixPath('/etc')

   *pathsegments* is specified similarly to :class:`PurePath`.

.. class:: WindowsPath(*pathsegments)

   A subclass of :class:`Path` and :class:`PureWindowsPath`, this class
   represents concrete Windows filesystem paths::

      >>> WindowsPath('c:/Program Files/')
      WindowsPath('c:/Program Files')

   *pathsegments* is specified similarly to :class:`PurePath`.

You can only instantiate the class flavour that corresponds to your system
(allowing system calls on non-compatible path flavours could lead to
bugs or failures in your application)::

   >>> import os
   >>> os.name
   'posix'
   >>> Path('setup.py')
   PosixPath('setup.py')
   >>> PosixPath('setup.py')
   PosixPath('setup.py')
   >>> WindowsPath('setup.py')
   Traceback (most recent call last):
     File "<stdin>", line 1, in <module>
     File "pathlib.py", line 798, in __new__
       % (cls.__name__,))
   NotImplementedError: cannot instantiate 'WindowsPath' on your system


Methods
^^^^^^^

Concrete paths provide the following methods in addition to pure paths
methods.  Many of these methods can raise an :exc:`OSError` if a system
call fails (for example because the path doesn't exist).

.. versionchanged:: 3.8

   :meth:`~Path.exists()`, :meth:`~Path.is_dir()`, :meth:`~Path.is_file()`,
   :meth:`~Path.is_mount()`, :meth:`~Path.is_symlink()`,
   :meth:`~Path.is_block_device()`, :meth:`~Path.is_char_device()`,
   :meth:`~Path.is_fifo()`, :meth:`~Path.is_socket()` now return ``False``
   instead of raising an exception for paths that contain characters
   unrepresentable at the OS level.


.. classmethod:: Path.cwd()

   Return a new path object representing the current directory (as returned
   by :func:`os.getcwd`)::

      >>> Path.cwd()
      PosixPath('/home/antoine/pathlib')


.. classmethod:: Path.home()

   Return a new path object representing the user's home directory (as
   returned by :func:`os.path.expanduser` with ``~`` construct)::

      >>> Path.home()
      PosixPath('/home/antoine')

   .. versionadded:: 3.5


.. method:: S3Path.stat()

   TODO nees clarification

   ::

      >>> p = Path('setup.py')
      >>> p.stat().st_size
      956
      >>> p.stat().st_mtime
      1327883547.852554


.. method:: Path.chmod(mode)

   Change the file mode and permissions, like :func:`os.chmod`::

      >>> p = Path('setup.py')
      >>> p.stat().st_mode
      33277
      >>> p.chmod(0o444)
      >>> p.stat().st_mode
      33060


.. method:: S3Path.exists()

   Whether the path points to an existing file or bucket::

      >> S3Path('./fake-key').exists()
      Will raise a ValueError
      >>> S3Path('.').exists()
      True
      >>> Path('setup.py').exists()
      True
      >>> Path('/etc').exists()
      True
      >>> Path('nonexistentfile').exists()
      False

   .. note::
      If the path points to a symlink, :meth:`exists` returns whether the
      symlink *points to* an existing file or directory.


.. method:: Path.expanduser()

   Return a new path with expanded ``~`` and ``~user`` constructs,
   as returned by :meth:`os.path.expanduser`::

      >>> p = PosixPath('~/films/Monty Python')
      >>> p.expanduser()
      PosixPath('/home/eric/films/Monty Python')

   .. versionadded:: 3.5


.. method:: Path.

(pattern)

   Glob the given *pattern* in the directory represented by this path,
   yielding all matching files (of any kind)::

      >>> sorted(Path('.').glob('*.py'))
      [S3Path('pathlib.py'), S3Path('setup.py'), S3Path('test_pathlib.py')]
      >>> sorted(S3Path('.').glob('*/*.py'))
      [S3Path('docs/conf.py')]

   The "``**``" pattern means "this directory and all subdirectories,
   recursively".  In other words, it enables recursive globbing::

      >>> sorted(S3Path('.').glob('**/*.py'))
      [S3Path('build/lib/pathlib.py'),
       S3Path('docs/conf.py'),
       S3Path('pathlib.py'),
       S3Path('setup.py'),
       S3Path('test_pathlib.py')]

   .. note::
      Using the "``**``" pattern in large directory trees may consume
      an inordinate amount of time.


.. method:: Path.group()

   Return the name of the group owning the file.  :exc:`KeyError` is raised
   if the file's gid isn't found in the system database.


.. method:: S3Path.is_dir()

   Return ``True`` if the path points to a directory (or a symbolic link
   pointing to a directory), ``False`` if it points to another kind of file.

   ``False`` is also returned if the path doesn't exist or is a broken symlink;
   other errors (such as permission errors) are propagated.


.. method:: S3Path.is_file()

   Return ``True`` if the path points to a regular file (or a symbolic link
   pointing to a regular file), ``False`` if it points to another kind of file.

   ``False`` is also returned if the path doesn't exist or is a broken symlink;
   other errors (such as permission errors) are propagated.


.. method:: Path.is_mount()

   Return ``True`` if the path is a :dfn:`mount point`: a point in a
   file system where a different file system has been mounted.  On POSIX, the
   function checks whether *path*'s parent, :file:`path/..`, is on a different
   device than *path*, or whether :file:`path/..` and *path* point to the same
   i-node on the same device --- this should detect mount points for all Unix
   and POSIX variants.  Not implemented on Windows.

   .. versionadded:: 3.7


.. method:: Path.is_symlink()

   Return ``True`` if the path points to a symbolic link, ``False`` otherwise.

   ``False`` is also returned if the path doesn't exist; other errors (such
   as permission errors) are propagated.


.. method:: Path.is_socket()

   Return ``True`` if the path points to a Unix socket (or a symbolic link
   pointing to a Unix socket), ``False`` if it points to another kind of file.

   ``False`` is also returned if the path doesn't exist or is a broken symlink;
   other errors (such as permission errors) are propagated.


.. method:: Path.is_fifo()

   Return ``True`` if the path points to a FIFO (or a symbolic link
   pointing to a FIFO), ``False`` if it points to another kind of file.

   ``False`` is also returned if the path doesn't exist or is a broken symlink;
   other errors (such as permission errors) are propagated.


.. method:: Path.is_block_device()

   Return ``True`` if the path points to a block device (or a symbolic link
   pointing to a block device), ``False`` if it points to another kind of file.

   ``False`` is also returned if the path doesn't exist or is a broken symlink;
   other errors (such as permission errors) are propagated.


.. method:: Path.is_char_device()

   Return ``True`` if the path points to a character device (or a symbolic link
   pointing to a character device), ``False`` if it points to another kind of file.

   ``False`` is also returned if the path doesn't exist or is a broken symlink;
   other errors (such as permission errors) are propagated.


.. method:: Path.iterdir()

   When the path points to a directory, yield path objects of the directory
   contents::

      >>> p = Path('docs')
      >>> for child in p.iterdir(): child
      ...
      PosixPath('docs/conf.py')
      PosixPath('docs/_templates')
      PosixPath('docs/make.bat')
      PosixPath('docs/index.rst')
      PosixPath('docs/_build')
      PosixPath('docs/_static')
      PosixPath('docs/Makefile')

.. method:: Path.lchmod(mode)

   Like :meth:`Path.chmod` but, if the path points to a symbolic link, the
   symbolic link's mode is changed rather than its target's.


.. method:: Path.lstat()

   Like :meth:`Path.stat` but, if the path points to a symbolic link, return
   the symbolic link's information rather than its target's.


.. method:: Path.mkdir(mode=0o777, parents=False, exist_ok=False)

   Create a new directory at this given path.  If *mode* is given, it is
   combined with the process' ``umask`` value to determine the file mode
   and access flags.  If the path already exists, :exc:`FileExistsError`
   is raised.

   If *parents* is true, any missing parents of this path are created
   as needed; they are created with the default permissions without taking
   *mode* into account (mimicking the POSIX ``mkdir -p`` command).

   If *parents* is false (the default), a missing parent raises
   :exc:`FileNotFoundError`.

   If *exist_ok* is false (the default), :exc:`FileExistsError` is
   raised if the target directory already exists.

   If *exist_ok* is true, :exc:`FileExistsError` exceptions will be
   ignored (same behavior as the POSIX ``mkdir -p`` command), but only if the
   last path component is not an existing non-directory file.

   .. versionchanged:: 3.5
      The *exist_ok* parameter was added.


.. method:: Path.open(mode='r', buffering=-1, encoding=None, errors=None, newline=None)

   Open the file pointed to by the path, like the built-in :func:`open`
   function does::

      >>> p = Path('setup.py')
      >>> with p.open() as f:
      ...     f.readline()
      ...
      '#!/usr/bin/env python3\n'


.. method:: Path.owner()

   Return the name of the user owning the file.  :exc:`KeyError` is raised
   if the file's uid isn't found in the system database.


.. method:: Path.read_bytes()

   Return the binary contents of the pointed-to file as a bytes object::

      >>> p = Path('my_binary_file')
      >>> p.write_bytes(b'Binary file contents')
      20
      >>> p.read_bytes()
      b'Binary file contents'

   .. versionadded:: 3.5


.. method:: Path.read_text(encoding=None, errors=None)

   Return the decoded contents of the pointed-to file as a string::

      >>> p = Path('my_text_file')
      >>> p.write_text('Text file contents')
      18
      >>> p.read_text()
      'Text file contents'

   The file is opened and then closed. The optional parameters have the same
   meaning as in :func:`open`.

   .. versionadded:: 3.5


.. method:: Path.rename(target)

   Rename this file or directory to the given *target*.  On Unix, if
   *target* exists and is a file, it will be replaced silently if the user
   has permission.  *target* can be either a string or another path object::

      >>> p = Path('foo')
      >>> p.open('w').write('some text')
      9
      >>> target = Path('bar')
      >>> p.rename(target)
      >>> target.open().read()
      'some text'


.. method:: Path.replace(target)

   Rename this file or directory to the given *target*.  If *target* points
   to an existing file or directory, it will be unconditionally replaced.


.. method:: Path.resolve(strict=False)

   Make the path absolute, resolving any symlinks.  A new path object is
   returned::

      >>> p = Path()
      >>> p
      PosixPath('.')
      >>> p.resolve()
      PosixPath('/home/antoine/pathlib')

   "``..``" components are also eliminated (this is the only method to do so)::

      >>> p = Path('docs/../setup.py')
      >>> p.resolve()
      PosixPath('/home/antoine/pathlib/setup.py')

   If the path doesn't exist and *strict* is ``True``, :exc:`FileNotFoundError`
   is raised.  If *strict* is ``False``, the path is resolved as far as possible
   and any remainder is appended without checking whether it exists.  If an
   infinite loop is encountered along the resolution path, :exc:`RuntimeError`
   is raised.

   .. versionadded:: 3.6
      The *strict* argument.

.. method:: Path.rglob(pattern)

   This is like calling :meth:`Path.glob` with "``**``" added in front of the
   given *pattern*::

      >>> sorted(Path().rglob("*.py"))
      [PosixPath('build/lib/pathlib.py'),
       PosixPath('docs/conf.py'),
       PosixPath('pathlib.py'),
       PosixPath('setup.py'),
       PosixPath('test_pathlib.py')]


.. method:: S3Path.rmdir()

   Remove this directory.  The directory must be empty.


.. method:: Path.samefile(other_path)

   Return whether this path points to the same file as *other_path*, which
   can be either a Path object, or a string.  The semantics are similar
   to :func:`os.path.samefile` and :func:`os.path.samestat`.

   An :exc:`OSError` can be raised if either file cannot be accessed for some
   reason.

   ::

      >>> p = Path('spam')
      >>> q = Path('eggs')
      >>> p.samefile(q)
      False
      >>> p.samefile('spam')
      True

   .. versionadded:: 3.5


.. method:: Path.symlink_to(target, target_is_directory=False)

   Make this path a symbolic link to *target*.  Under Windows,
   *target_is_directory* must be true (default ``False``) if the link's target
   is a directory.  Under POSIX, *target_is_directory*'s value is ignored.

   ::

      >>> p = Path('mylink')
      >>> p.symlink_to('setup.py')
      >>> p.resolve()
      PosixPath('/home/antoine/pathlib/setup.py')
      >>> p.stat().st_size
      956
      >>> p.lstat().st_size
      8

   .. note::
      The order of arguments (link, target) is the reverse
      of :func:`os.symlink`'s.


.. method:: Path.touch(mode=0o666, exist_ok=True)

   Create a file at this given path.  If *mode* is given, it is combined
   with the process' ``umask`` value to determine the file mode and access
   flags.  If the file already exists, the function succeeds if *exist_ok*
   is true (and its modification time is updated to the current time),
   otherwise :exc:`FileExistsError` is raised.


.. method:: Path.unlink()

   Remove this file or symbolic link.  If the path points to a directory,
   use :func:`Path.rmdir` instead.


.. method:: Path.write_bytes(data)

   Open the file pointed to in bytes mode, write *data* to it, and close the
   file::

      >>> p = Path('my_binary_file')
      >>> p.write_bytes(b'Binary file contents')
      20
      >>> p.read_bytes()
      b'Binary file contents'

   An existing file of the same name is overwritten.

   .. versionadded:: 3.5


.. method:: Path.write_text(data, encoding=None, errors=None)

   Open the file pointed to in text mode, write *data* to it, and close the
   file::

      >>> p = Path('my_text_file')
      >>> p.write_text('Text file contents')
      18
      >>> p.read_text()
      'Text file contents'

   .. versionadded:: 3.5

Correspondence to tools in the :mod:`os` module
-----------------------------------------------

Below is a table mapping various :mod:`os` functions to their corresponding
:class:`PurePath`/:class:`Path` equivalent.

.. note::

   Although :func:`os.path.relpath` and :meth:`PurePath.relative_to` have some
   overlapping use-cases, their semantics differ enough to warrant not
   considering them equivalent.

====================================   ==============================
os and os.path                         pathlib
====================================   ==============================
:func:`os.path.abspath`                :meth:`Path.resolve`
:func:`os.chmod`                       :meth:`Path.chmod`
:func:`os.mkdir`                       :meth:`Path.mkdir`
:func:`os.rename`                      :meth:`Path.rename`
:func:`os.replace`                     :meth:`Path.replace`
:func:`os.rmdir`                       :meth:`Path.rmdir`
:func:`os.remove`, :func:`os.unlink`   :meth:`Path.unlink`
:func:`os.getcwd`                      :func:`Path.cwd`
:func:`os.path.exists`                 :meth:`Path.exists`
:func:`os.path.expanduser`             :meth:`Path.expanduser` and
                                       :meth:`Path.home`
:func:`os.path.isdir`                  :meth:`Path.is_dir`
:func:`os.path.isfile`                 :meth:`Path.is_file`
:func:`os.path.islink`                 :meth:`Path.is_symlink`
:func:`os.stat`                        :meth:`Path.stat`,
                                       :meth:`Path.owner`,
                                       :meth:`Path.group`
:func:`os.path.isabs`                  :meth:`PurePath.is_absolute`
:func:`os.path.join`                   :func:`PurePath.joinpath`
:func:`os.path.basename`               :data:`PurePath.name`
:func:`os.path.dirname`                :data:`PurePath.parent`
:func:`os.path.samefile`               :meth:`Path.samefile`
:func:`os.path.splitext`               :data:`PurePath.suffix`
====================================   ==============================




