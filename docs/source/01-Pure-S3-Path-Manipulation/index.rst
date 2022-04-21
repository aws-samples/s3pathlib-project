.. _pure-s3-path-manipulation:

Pure S3 Path Manipulation
==============================================================================

.. contents::
    :class: this-will-duplicate-information-and-it-is-still-useful-here
    :depth: 1
    :local:


What is Pure S3 Path
------------------------------------------------------------------------------
Pure S3 Path is just a python object representing an AWS S3 bucket, object, folder, etc. It doesn't call ANY AWS API and also doesn't mean that the corresponding S3 object exists.


S3 Path Concepts
------------------------------------------------------------------------------
S3 Path is a logical concept. It can map to different AWS S3 concepts. Here is the list of common S3 Path type.

1. **Classic S3 object**: represents a s3 object, such as ``s3://bucket/folder/file.txt``.
2. **Logical S3 directory**: represents a s3 directory, such as ``s3://bucket/folder/``.
3. **S3 bucket**: represents a s3 bucket, such as ``s3://bucket/``
4. **Void Path**: no bucket, no key, no nothing.
5. **Relative Path**: for example, the relative path from ``s3://bucket/folder/file.txt`` to ``s3://bucket/`` **IS** ``folder/file.txt``. A relative path can be joined with other S3 Path to form another S3 Path. Void path is also a very special relative path. Any concrete path join a void path results to itself.
6. **Concrete Path**: a S3 Path really mean something can physically exists. Any classic S3 object path, logical S3 directory path and S3 bucket is a concrete Path. A concrete path join a relative path results another concrete path.


Construct a S3 Path
------------------------------------------------------------------------------
**The most intuitive way would be from string**, here's a Classic AWS S3 object example:

.. code-block:: python

    # import
    >>> from s3pathlib import S3Path

    # construct from string, auto join parts
    >>> p = S3Path("bucket", "folder", "file.txt")

    # print
    >>> p
    S3Path('s3://bucket/folder/file.txt')

    # construct from full path also works
    >>> S3Path("bucket/folder/file.txt")
    S3Path('s3://bucket/folder/file.txt')

**Directory is only logical concept in AWS S3**. AWS uses ``/`` as the path delimiter in S3 key. Here's a Logical AWS S3 directory example, use ``/`` at the end to indicate that it is a directory:

.. code-block:: python

    # construct from string, auto join parts
    >>> S3Path("bucket", "folder", "subfolder/")
    S3Path('s3://bucket/folder/subfolder/')

**Summary**:

- The **first** non-void argument defines the ``bucket``
- The **last** non-void argument defines whether it is a ``directory`` or a ``object``

**From S3 URI or S3 ARN**

S3 URI is a unique resource identifier that uniquely locate a S3 bucket, S3 object or S3 directory. ARN is Amazon Resource Namespace that also locate a unique AWS Resource such as a S3 bucket, S3 object or S3 directory.

.. code-block:: python

    >>> S3Path.from_s3_uri("s3://bucket/folder/file.txt")
    S3Path('s3://bucket/folder/file.txt')

    >>> S3Path.from_s3_arn("arn:aws:s3:::bucket/folder/file.txt")
    S3Path('s3://bucket/folder/file.txt')


S3 Path Attributes
------------------------------------------------------------------------------
:class:`~s3pathlib.core.S3Path` is immutable and hashable. These attributes doesn't need AWS boto3 API call and generally available. For attributes like :attr:`~s3pathlib.core.S3Path.etag`, :attr:`~s3pathlib.core.S3Path.size` that need API call, see :ref:`configure-aws-context`

.. code-block:: python

    # create an instance
    >>> p = S3Path("bucket", "folder", "file.txt")

- :attr:`~s3pathlib.core.S3Path.bucket`

.. code-block:: python

    >>> p.bucket
    'bucket'

- :attr:`~s3pathlib.core.S3Path.key`

.. code-block:: python

    >>> p.key
    'folder/file.txt'

- :attr:`~s3pathlib.core.S3Path.parts`: you can access the s3 key parts in sequence too

.. code-block:: python

    >>> p.parts
    ['folder', 'file.txt']

Since it is **immutable**, you cannot change the value of the attribute:

.. code-block:: python

    >>> p = S3Path("bucket", "folder", "file.txt")
    >>> p.bucket = "new-bucket"
    Traceback (most recent call last):
      File "<input>", line 1, in <module>
    AttributeError: can't set attribute

- :attr:`~s3pathlib.core.S3Path.uri`: `unique resource identifier <https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-bucket-intro.html>`_

.. code-block:: python

    >>> p.uri
    's3://bucket/folder/file.txt'

- :attr:`~s3pathlib.core.S3Path.console_url`: open console to preview

.. code-block:: python

    >>> p.console_url
    'https://s3.console.aws.amazon.com/s3/object/bucket?prefix=folder/file.txt'

- :attr:`~s3pathlib.core.S3Path.arn`: `aws resource namespace <https://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html>`_

.. code-block:: python

    >>> p.arn
    'arn:aws:s3:::bucket/folder/file.txt'

Logically a :class:`~s3pathlib.core.S3Path` is also a file system like object. So it should have those **file system concepts** too:

.. code-block:: python

    # create an instance
    >>> p = S3Path("bucket", "folder", "file.txt")

- :attr:`~s3pathlib.core.S3Path.basename`: the file name with extension.

.. code-block:: python

    >>> p.basename
    'file.txt'

- :attr:`~s3pathlib.core.S3Path.fname`: file name without file extension.

.. code-block:: python

    >>> p.fname
    'file'

- :attr:`~s3pathlib.core.S3Path.ext`: file extension, if available

.. code-block:: python

    >>> p.ext
    '.txt'

- :attr:`~s3pathlib.core.S3Path.dirname`: the basename of the parent directory

.. code-block:: python

    >>> p.dirname
    'folder'

- :attr:`~s3pathlib.core.S3Path.abspath`: the absolute path is the full path from the root drive. You can think of S3 bucket as the root drive.

.. code-block:: python

    >>> p.abspath
    '/folder/file.txt'

- :attr:`~s3pathlib.core.S3Path.parent`: the parent directory S3 Path

.. code-block:: python

    >>> p.parent
    S3Path('s3://bucket/folder/')

- :attr:`~s3pathlib.core.S3Path.dirpath`: the absolute path of the parent directory. It is equal to ``p.parent.abspath``

.. code-block:: python

    >>> p.dirpath
    '/folder/'


S3 Path Methods
------------------------------------------------------------------------------
**Identify S3Path type**

- :meth:`~s3pathlib.core.S3Path.is_dir`:

.. code-block:: python

    >>> S3Path("bucket", "folder/").is_dir()
    True

- :meth:`~s3pathlib.core.S3Path.is_file`:

.. code-block:: python

    >>> S3Path("bucket", "file.txt").is_file()
    True

- :meth:`~s3pathlib.core.S3Path.is_bucket`:

.. code-block:: python

    >>> S3Path("bucket").is_bucket()
    True

- :meth:`~s3pathlib.core.S3Path.is_void`:

.. code-block:: python

    >>> S3Path().is_void()
    True

- :meth:`~s3pathlib.core.S3Path.is_relpath`:

.. code-block:: python

    >>> S3Path("bucket", "folder/").relative_to(S3Path("bucket")).is_relpath()
    True

**Comparison**

Since S3Path can convert to S3 URI, it should be able to compare to each other.

.. code-block:: python

    >>> S3Path("bucket/file.txt") == S3Path("bucket/file.txt")
    True

    >>> S3Path("bucket") == S3Path("bucket")
    True

    >>> S3Path("bucket1") == S3Path("bucket2")
    False

    >>> S3Path("bucket1") < S3Path("bucket2")
    True

    >>> S3Path("bucket1") <= S3Path("bucket2")
    True

    >>> S3Path("bucket/a/1.txt") > S3Path("bucket/a/")
    True

    >>> S3Path("bucket/a/1.txt") < S3Path("bucket/a/2.txt")
    True

**Hash**

``S3Path`` is :meth:`hashable <~s3pathlib.core.S3Path.__hash__>`.

.. code-block:: python

    >>> p1 = S3Path("bucket", "1.txt")
    >>> p2 = S3Path("bucket", "2.txt")
    >>> p3 = S3Path("bucket", "3.txt")
    >>> set1 = {p1, p2}
    >>> set2 = {p2, p3}

    # union
    >>> set1.union(set2)
    {S3Path('s3://bucket/1.txt'), S3Path('s3://bucket/2.txt'), S3Path('s3://bucket/3.txt')}

    # intersection
    >>> set1.intersection(set2)
    {S3Path('s3://bucket/2.txt')}

    # difference
    >>> set1.difference(set2)
    {S3Path('s3://bucket/1.txt')}

**Mutate the immutable S3Path**

- :meth:`~s3pathlib.core.S3Path.copy`: create a copy of this S3Path, but completely different because it is immutable.

.. code-block:: python

    >>> p1 = S3Path("bucket", "folder", "file.txt")
    >>> p2 = p1.copy()

    >>> p1 == p2
    True

    >>> p1 is p2
    False

- :meth:`~s3pathlib.core.S3Path.change`: Create a new S3Path by replacing part of the attributes.

.. code-block:: python

    >>> p = S3Path("bkt", "a", "b", "c.jpg")

    >>> p.change(new_bucket="bkt1").uri
    's3://bkt1/a/b/c.jpg'

    >>> p.change(new_abspath="x/y/z.png").uri
    's3://bkt/x/y/z.png'

    >>> p.change(new_ext=".png").uri
    's3://bkt/a/b/c.png'

    >>> p.change(new_fname="d").uri
    's3://bkt/a/b/d.jpg'

    >>> p.change(new_basename="d.png").uri
    's3://bkt/a/b/d.png'
    >>> p1.is_dir()
    False

    >>> p.change(new_basename="d/").uri
    's3://bkt/a/b/d/'
    >>> p1.is_dir()
    True

    >>> p.change(new_dirname="d/").uri
    's3://bkt/a/d/c.jpg'

    >>> p.change(new_dirpath="x/y/").uri
    's3://bkt/x/y/c.jpg'

- :meth:`~s3pathlib.core.S3Path.join_path`: join with other relative paths to form another path

.. code-block:: python

    # create some s3path
    >>> p1 = S3Path("bucket", "folder", "subfolder", "file.txt")
    >>> p2 = p1.parent
    >>> relpath1 = p1.relative_to(p2)

    # preview value
    >>> p1
    S3Path('s3://bucket/folder/subfolder/file.txt')
    >>> p2
    S3Path('s3://bucket/folder/subfolder/')
    >>> relpath1
    S3Path('file.txt')

    # join one relative path
    >>> p2.join_path(relpath1)
    S3Path('s3://bucket/folder/subfolder/file.txt')

    # join multiple relative path
    >>> p3 = p2.parent
    >>> relpath2 = p2.relative_to(p3)
    >>> p3.join_path(relpath2, relpath1)
    S3Path('s3://bucket/folder/subfolder/file.txt')

**Parent relationship**

- :meth:`~s3pathlib.core.S3Path.relative_to`: calculate the relative path between two path, the "to path" has to be "shorter than" the "from path"

.. code-block:: python

    >>> S3Path("bucket", "a/b/c").relative_to(S3Path("bucket", "a")).parts
    ['b', 'c']

    >>> S3Path("bucket", "a").relative_to(S3Path("bucket", "a")).parts
    []

    >>> S3Path("bucket", "a").relative_to(S3Path("bucket", "a/b/c")).parts
    ValueError ...


What's Next
------------------------------------------------------------------------------

Since then everything is not talking to AWS yet, let's learn how to make some AWS S3 API call using ``s3pathlib``.

Go :ref:`stateless-s3-api`
