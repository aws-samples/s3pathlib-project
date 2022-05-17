.. image:: https://readthedocs.org/projects/s3pathlib/badge/?version=latest
    :target: https://s3pathlib.readthedocs.io/en/latest/
    :alt: Documentation Status

.. image:: https://github.com/aws-samples/s3pathlib-project/workflows/CI/badge.svg
    :target: https://github.com/aws-samples/s3pathlib-project/actions?query=workflow:CI

.. image:: https://img.shields.io/pypi/v/s3pathlib.svg
    :target: https://pypi.python.org/pypi/s3pathlib

.. image:: https://img.shields.io/pypi/l/s3pathlib.svg
    :target: https://pypi.python.org/pypi/s3pathlib

.. image:: https://img.shields.io/pypi/pyversions/s3pathlib.svg
    :target: https://pypi.python.org/pypi/s3pathlib

------

.. image:: https://img.shields.io/badge/Link-Document-orange.svg
    :target: https://s3pathlib.readthedocs.io/en/latest/

.. image:: https://img.shields.io/badge/Link-API-blue.svg
    :target: https://s3pathlib.readthedocs.io/en/latest/py-modindex.html

.. image:: https://img.shields.io/badge/Link-Source_Code-blue.svg
    :target: https://s3pathlib.readthedocs.io/en/latest/py-modindex.html

.. image:: https://img.shields.io/badge/Link-Submit_Issue-blue.svg
    :target: https://github.com/aws-samples/s3pathlib-project/issues

.. image:: https://img.shields.io/badge/Link-Request_Feature-blue.svg
    :target: https://github.com/aws-samples/s3pathlib-project/issues

.. image:: https://img.shields.io/badge/Link-Download-blue.svg
    :target: https://pypi.org/pypi/s3pathlib#files


Welcome to ``s3pathlib`` Documentation
==============================================================================

``s3pathlib`` is the python package provides the Pythonic objective oriented programming (OOP) interface to manipulate AWS S3 object / directory. The api is similar to the ``pathlib`` `standard library <https://docs.python.org/3/library/pathlib.html>`_ and very intuitive for human.

.. note::

    You may not viewing the full document, `FULL DOCUMENT IS HERE <https://s3pathlib.readthedocs.io/en/latest/>`_


Quick Start
------------------------------------------------------------------------------
.. note::

    `COMPREHENSIVE DOCUMENT guide / features / best practice can be found at HERE <https://s3pathlib.readthedocs.io/en/latest/#full-table-of-content>`_


**Import the library, declare a S3 object**

.. code-block:: python

    # import
    >>> from s3pathlib import S3Path

    # construct from string, auto join parts
    >>> p = S3Path("bucket", "folder", "file.txt")
    >>> p.bucket
    'bucket'
    >>> p.key
    'folder/file.txt'
    >>> p.uri
    's3://bucket/folder/file.txt'
    >>> p.console_url # click to preview it in AWS console
    'https://s3.console.aws.amazon.com/s3/object/bucket?prefix=folder/file.txt'
    >>> p.arn
    'arn:aws:s3:::bucket/folder/file.txt'

**Talk to AWS S3 and get some information**

.. code-block:: python

    # s3pathlib maintains a "context" object that holds the AWS authentication information
    # you just need to build your own boto session object and attach to it
    >>> import boto3
    >>> from s3pathlib import context
    >>> context.attach_boto_session(
    ...     boto3.session.Session(
    ...         region_name="us-east-1",
    ...         profile_name="my_aws_profile",
    ...     )
    ... )

    >>> p = S3Path("bucket", "folder", "file.txt")
    >>> p.etag
    '3e20b77868d1a39a587e280b99cec4a8'
    >>> p.size
    56789000
    >>> p.size_for_human
    '51.16 MB'

    # folder works too, you just need to use a tailing "/" to identify that
    >>> p = S3Path("bucket", "datalake/")
    >>> p.count_objects()
    7164 # number of files under this prefix
    >>> p.calculate_total_size()
    (7164, 236483701963) # 7164 objects, 220.24 GB
    >>> p.calculate_total_size(for_human=True)
    (7164, '220.24 GB') # 7164 objects, 220.24 GB

**Manipulate Folder in S3**

Native S3 Write API (those operation that change the state of S3) only operate on object level. And the `list_objects <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.list_objects_v2>`_ API returns 1000 objects at a time. You need additional effort to manipulate objects recursively. ``s3pathlib`` **CAN SAVE YOUR LIFE**

.. code-block:: python

    # create a S3 folder
    >>> p = S3Path("bucket", "github", "repos", "my-repo/")

    # upload all python file from /my-github-repo to s3://bucket/github/repos/my-repo/
    >>> p.upload_dir("/my-repo", pattern="**/*.py", overwrite=False)

    # copy entire s3 folder to another s3 folder
    >>> p2 = S3Path("bucket", "github", "repos", "another-repo/")
    >>> p1.copy_to(p2, overwrite=True)

    # delete all objects in the folder, recursively, to clean up your test bucket
    >>> p.delete_if_exists()
    >>> p2.delete_if_exists()

**S3 Path Filter**

Ever think of filter S3 object by it's attributes like: dirname, basename, file extension, etag, size, modified time? It is supposed to be simple in Python:

.. code-block:: python

    >>> root = S3Path("bucket") # assume you have a lots of files in this bucket
    >>> iterproxy = root.iter_objects().filter(
    ...     S3Path.size >= 10_000_000, S3Path.ext == ".csv" # add filter
    ... )

    >>> iterproxy.one() # fetch one
    S3Path('s3://bucket/larger-than-10MB-1.csv')

    >>> iterproxy.many(3) # fetch three
    [
        S3Path('s3://bucket/larger-than-10MB-1.csv'),
        S3Path('s3://bucket/larger-than-10MB-2.csv'),
        S3Path('s3://bucket/larger-than-10MB-3.csv'),
    ]

    >>> for p in iterproxy: # iter the rest
    ...     print(p)


**File Like Object for Simple IO**

``S3Path`` is file-like object. It support ``open`` and context manager syntax out of the box. Here are only some highlight examples:

.. code-block:: python

    # Stream big file by line
    >>> p = S3Path("bucket", "log.txt")
    >>> with p.open("r") as f:
    ...     for line in f:
    ...         do what every you want

    # JSON io
    >>> import json
    >>> p = S3Path("bucket", "config.json")
    >>> with p.open("w") as f:
    ...     json.dump({"password": "mypass"}, f)

    # pandas IO
    >>> import pandas as pd
    >>> p = S3Path("bucket", "dataset.csv")
    >>> df = pd.DataFrame(...)
    >>> with p.open("w") as f:
    ...     df.to_csv(f)


Getting Help
------------------------------------------------------------------------------
Please use the ``python-s3pathlib`` tag on Stack Overflow to get help.

Submit a ``I want help`` issue tickets on `GitHub Issues <https://github.com/aws-samples/s3pathlib-project/issues/new/choose>`_



Contributing
------------------------------------------------------------------------------
Please see the `Contribution Guidelines <https://github.com/aws-samples/s3pathlib-project/blob/main/CONTRIBUTING.rst>`_.


Copyright
------------------------------------------------------------------------------
s3pathlib is an open source project. See the `LICENSE <https://github.com/aws-samples/s3pathlib-project/blob/main/LICENSE>`_ file for more information.
