Stateful S3 API
==============================================================================

.. contents::
    :class: this-will-duplicate-information-and-it-is-still-useful-here
    :depth: 1
    :local:


What is Stateful S3 API
------------------------------------------------------------------------------
Comparing to :ref:`Stateless S3 API <what-is-stateless-s3-api>`, **Stateful S3 API will change the state of the S3**. For example, upload a file, put object, copy object, delete object.

``s3pathlib`` provides a Pythonic way to manipulate AWS S3 Object by file or by directory.


Copy, Move (Cut), Delete, Upload
------------------------------------------------------------------------------
:class:`~s3pathlib.core.s3path.S3Path` is a OS path-liked object. So you should be able to, copy, move (cut), delete, overwrite.

- :meth:`~s3pathlib.core.copy.CopyAPIMixin.copy_to`: copy object or directory (recursively) from one location to another. similar to `shutil.copy <https://docs.python.org/3/library/shutil.html#shutil.copy>`_ and `shutil.copytree <https://docs.python.org/3/library/shutil.html#shutil.copytree>`_

.. code-block:: python

    # copy files
    # from s3://bucket/datalake/table_transactions/
    # to s3://backup-bucket/datalake/table_transactions/
    >>> p = S3Path("bucket", "datalake", "table_transactions/")
    >>> p.copy_to(p.change(new_bucket="backup-bucket"))

- :meth:`~s3pathlib.core.copy.CopyAPIMixin.move_to`: move (cut) object or directory (recursively) from one location to another. similar to `shutil.move <https://docs.python.org/3/library/shutil.html#shutil.move>`_

.. code-block:: python

    # move files
    # from s3://bucket/datalake/table_transactions/
    # to s3://backup-bucket/datalake/table_transactions/
    >>> p = S3Path("bucket", "datalake", "table_transactions/")
    >>> p.move_to(p.change(new_bucket="backup-bucket"))

- :meth:`~s3pathlib.core.delete.DeleteAPIMixin.delete_if_exists`: delete object or directory (recursively). similar to `os.remove <https://docs.python.org/3/library/os.html#os.remove>`_ and `shutil.rmtree <https://docs.python.org/3/library/shutil.html#shutil.rmtree>`_

.. code-block:: python

    # delete all files in s3://bucket/tmp/*.*
    >>> p = S3Path("bucket", "tmp/")
    >>> p.delete_if_exists()

- :meth:`~s3pathlib.core.upload.UploadAPIMixin.upload_file`: upload a file to s3.

.. code-block:: python

    >>> p = S3Path("bucket", "folder", "file.txt")
    >>> if not p.exists():
    ...     p.upload_file("/tmp/log.txt")

- :meth:`~s3pathlib.core.upload.UploadAPIMixin.upload_dir`: upload a directory (recursively) to a s3 prefix.

.. code-block:: python

    >>> p = S3Path("bucket", "my-github-repo")
    >>> p.upload_dir("/tmp/my-github-repo", overwrite=False)
