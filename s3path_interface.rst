Pure paths:
===========

Full basic PurePath documentation linked here: `PurePathDocs`_.

**PureS3Path(*pathsegments):**

A subclass of `PurePath`_, this path flavour represents AWS S3 Service semantics.

   >>> PureS3Path('/<bucket>/<key>')

pathsegments is specified similarly to `PurePath`_.

PureS3Path have a similar behavior like `PurePosixPath`_ except for the below changes:
--------------------------------------------------------------------------------------

Double dots (``'..'``) are treated as follows.
This is different then PurePath since AWS S3 Service don't support symbolic links::

   >>> PureS3Path('foo/../bar')
   PureS3Path('bar')

**PureS3Path.as_uri()**

Represent the path as a AWS S3 URI. `ValueError`_ is raised if the path isn't absolute::

  >>> p = PureS3Path('/pypi-proxy/boto3/')
  >>> p.as_uri()
  's3://pypi-proxy/boto3/'
  >>> p = PureS3Path('/pypi-proxy/boto3/index.html')
  >>> p.as_uri()
  's3://pypi-proxy/boto3/index.html'

**PureS3Path.from_uri(uri)**

Represent a AWS S3 URI as a PureS3Path::

   >>> PureS3Path.from_uri('s3://pypi-proxy/boto3/')
   PureS3Path('/pypi-proxy/boto3/')

This is a new class method.

**PureS3Path.bucket**

The Bucket path.  If a path don't have a key return `None`.
`ValueError`_ is raised if the path isn't absolute::

   >>> p = PureS3Path.from_uri('s3://pypi-proxy/boto3/').bucket
   PureS3Path('/pypi-proxy/')
   >>> p = PureS3Path('/').bucket
   None

This is a new property.

**PureS3Path.key**

The Key path. If a path don't have a key return `None`.
`ValueError`_ is raised if the path isn't absolute::

   >>> p = PureS3Path('/pypi-proxy/boto3/').key
   PureS3Path('boto3')
   >>> PureS3Path('/pypi-proxy/boto3/index.html').key
   PureS3Path('boto3/index.html')
   >>> p = PureS3Path.from_uri('s3://pypi-proxy/').key
   None

This is a new property.


**I'm HERE:!!!!!!!!!!!!!!!!!!!!**


.. _PurePathDocs : https://docs.python.org/3/library/pathlib.html#pure-paths
.. _PurePath: https://docs.python.org/3/library/pathlib.html#pathlib.PurePath
.. _PurePosixPath: https://docs.python.org/3/library/pathlib.html#pathlib.PurePosixPath
.. _ValueError: https://docs.python.org/3/library/exceptions.html#ValueError



Concrete paths:
===============

Concrete paths are subclasses of the pure path classes.  In addition to
operations provided by the latter, they also provide methods to do system
calls on path objects.  There are three ways to instantiate concrete paths:

**S3Path(*pathsegments)**

A subclass of :class:'PureS3Path', this class represents concrete paths of
the system's path flavour (instantiating it creates either a
:class:'S3Path')::

  >>> S3Path('setup.py')
  S3Path('setup.py')





Methods:
========

Concrete paths provide the following methods in addition to pure paths
methods.  Many of these methods can raise an :exc:'OSError' if a system
call fails (for example because the path doesn't exist).


..    .. versionadded:: 3.5

**S3Path.stat()**

   TODO nees clarification

   ::

  >>> p = S3Path('setup.py')
  >>> p.stat().st_size
  956
  >>> p.stat().st_mtime
  1327883547.852554



**S3Path.exists()**

Whether the path points to an existing file or bucket::

  >> S3Path('./fake-key').exists()
  Will raise a ValueError
  >>> S3Path('.').exists()
  True
  >>> S3Path('setup.py').exists()
  True
  >>> S3Path('/etc').exists()
  True
  >>> S3Path('nonexistentfile').exists()
  False

   .. note::
      If the path points to a symlink, :meth:`exists` returns whether the
      symlink *points to* an existing file or directory.


**Path.expanduser()

Return a new path with expanded ``~`` and ``~user`` constructs,
as returned by :meth:`os.path.expanduser`::

  >>> p = PosixPath('~/films/Monty Python')
  >>> p.expanduser()
  PosixPath('/home/eric/films/Monty Python')

       .. versionadded:: 3.5


**Path**.

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


**S3Path.group()

Return the name of the group owning the file.  :exc:`KeyError` is raised
if the file's gid isn't found in the system database.


**S3Path.is_dir()

Return ``True`` if the path points to a directory (or a symbolic link
pointing to a directory), ``False`` if it points to another kind of file.

``False`` is also returned if the path doesn't exist or is a broken symlink;
other errors (such as permission errors) are propagated.


**S3Path.is_file()

Return ``True`` if the path points to a regular file (or a symbolic link
pointing to a regular file), ``False`` if it points to another kind of file.

``False`` is also returned if the path doesn't exist or is a broken symlink;
other errors (such as permission errors) are propagated.


**S3Path.is_mount()

   Returns ``False`` in S3Path.

       .. versionadded:: 3.7



**S3Path.iterdir()

When the path points to a directory, yield path objects of the directory
contents::

  >>> p = S3Path('docs')
  >>> for child in p.iterdir(): child
  ...
  S3Path('docs/conf.py')
  S3Path('docs/_templates')
  S3Path('docs/make.bat')
  S3Path('docs/index.rst')
  S3Path('docs/_build')
  S3Path('docs/_static')
  S3Path('docs/Makefile')

       .. versionchanged:: 3.5
          The *exist_ok* parameter was added.


**S3Path.open(mode='r', buffering=-1, encoding=None, errors=None, newline=None)

Open the file pointed to by the path, like the built-in :func:`open`
function does::

  >>> p = S3Path('setup.py')
  >>> with p.open() as f:
  ...     f.readline()
  ...
  '#!/usr/bin/env python3\n'


**S3Path.owner()

Return the name of the owner's DisplayName.::

**S3Path.read_bytes()

Return the binary contents of the pointed-to file as a bytes object::

  >>> p = S3Path('my_binary_file')
  >>> p.write_bytes(b'Binary file contents')
  20
  >>> p.read_bytes()
  b'Binary file contents'

       .. versionadded:: 3.5


**Path.read_text(encoding=None, errors=None)

Return the decoded contents of the pointed-to file as a string::

  >>> p = S3Path('my_text_file')
  >>> p.write_text('Text file contents')
  18
  >>> p.read_text()
  'Text file contents'

The file is opened and then closed. The optional parameters have the same
meaning as in :func:`open`.

       .. versionadded:: 3.5


**S3Path.rename(target)

Rename this file or directory to the given *target*.::

  >>> p = S3Path('foo')
  >>> p.open('w').write('some text')
  9
  >>> target = Path('bar')
  >>> p.rename(target)
  >>> target.open().read()
  'some text'


**S3Path('/test-bucket/docs/').replace(target)

Rename this file or directory to the given *target*.  If *target* points
to an existing file or directory, it will be unconditionally replaced.



**S3Path.rmdir()

 Remove this directory.  The directory must be empty.


**S3Path.samefile(other_path)

Return whether this path points to the same file as *other_path*, which
can be either a S3Path object, or a string.  The semantics are similar
to :func:`os.path.samefile` and :func:`os.path.samestat`.

An :exc:`OSError` can be raised if either file cannot be accessed for some
reason.

::

  >>> p = S3Path('bucket/file')
  >>> q = S3Path('other/file')
  >>> p.samefile(q)
  False
  >>> p.samefile('bucket/file')
  True


**S3Path.touch(mode=0o666, exist_ok=True)

   Create a file at this given path.  If *mode* is given, it is combined
   with the process' ``umask`` value to determine the file mode and access
   flags.  If the file already exists, the function succeeds if *exist_ok*
   is true (and its modification time is updated to the current time),
   otherwise :exc:`FileExistsError` is raised.


**S3Path.write_bytes(data)

Open the file pointed to in bytes mode, write *data* to it, and close the
file::

  >>> p = Path('my_binary_file')
  >>> p.write_bytes(b'Binary file contents')
  20
  >>> p.read_bytes()
  b'Binary file contents'

An existing file of the same name is overwritten.

       .. versionadded:: 3.5


**S3Path.write_text(data, encoding=None, errors=None)

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


    .. versionchanged:: 3.6
      Added support for the :class:'os.PathLike' interface.
