.. _stateless-s3-api:

Stateless S3 API
==============================================================================

.. contents::
    :class: this-will-duplicate-information-and-it-is-still-useful-here
    :depth: 1
    :local:


.. _what-is-stateless-s3-api:

What is Stateless S3 API
------------------------------------------------------------------------------
There are lots of `AWS S3 API <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html>`_ available. But some of them only retrieve information from the server, but never change the state of the S3 bucket. No bucket, files are moved, changed, deleted, so called **Stateless S3 API**.

For example, you can use stateless S3 api to get the metadata of the object.


.. _configure-aws-context:

Configure AWS Context
------------------------------------------------------------------------------
If you are running the code from your laptop, you need a AWS API key pair for authentication. Follow this official guide to configure the credential on your laptop.

- `Configure the AWS Credential for your local laptop <https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html>`_

``s3pathlib`` use a context object to store the global runtime data including the pre-authenticated boto session. It creates a default session using your default credential (if available). However, you can always configure the boto session yourself and attach it to the context. You can find how below:

- `Create custom boto session <https://boto3.amazonaws.com/v1/documentation/api/latest/guide/session.html>`_

.. code-block:: python

    import boto3
    from s3pathlib import context

    context.attach_boto_session(
        boto3.session.Session(
            region_name="us-east-1",
            profile_name="my_aws_profile",
        )
    )

If you are running the code from Cloud machine like AWS EC2 or AWS Lambda, follow this official guide to grant your computational machine propert AWS S3 access.

- `Grant AWS EC2 API access using IAM Role <https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html>`_

For more advanced AWS credential manipulation, please see :ref:`aws-credential`.


Get S3 Object Metadata
------------------------------------------------------------------------------
See definition of server side object metadata here: https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html.

- :attr:`~s3pathlib.core.metadata.MetadataAPIMixin.etag`
- :attr:`~s3pathlib.core.metadata.MetadataAPIMixin.last_modified_at`
- :attr:`~s3pathlib.core.metadata.MetadataAPIMixin.size`
- :attr:`~s3pathlib.core.metadata.MetadataAPIMixin.size_for_human`
- :attr:`~s3pathlib.core.metadata.MetadataAPIMixin.version_id`
- :attr:`~s3pathlib.core.metadata.MetadataAPIMixin.expire_at`

.. note::

    S3 object metadata are cached only on API call. If you want to get the latest server side value, you can call :meth:`~s3pathlib.core.metadata.MetadataAPIMixin.clear_cache()` method and then moving forward.

    .. code-block:: python
    
        >>> p = S3Path("bucket", "file.txt")
        >>> p.etag
        'aaa...'

        >>> # you did something like put_object
        >>> p.clear_cache()
        >>> p.etag
        'bbb...'


Check if Exists
------------------------------------------------------------------------------
You can test if:

- For **S3 bucket**: check if the bucket exists. If you don't have the access, then it raise exception.
- For **S3 object**: check if the object exists
- For **S3 directory**: since S3 directory is a logical concept and never physically exists. It returns True only if there is at least one object under this directory (prefix)
- You cannot check existence for Void path and Relative path.

Example:

.. code-block:: python

    # check if the bucket exists
    >>> S3Path("bucket").exists()

    # check if the object exists
    >>> S3Path("bucket", "folder/file.txt").exists()

    # check if the directory has at least one file
    >>> S3Path("bucket", "folder/").exists()


Count Objects and Size
------------------------------------------------------------------------------
AWS Console has a button "Calculate Total Size" tells you how many objects and the total size in a S3 folder. :meth:`~s3pathlib.core.iter_objects.IterObjectsAPIMixin.calculate_total_size` and :meth:`~s3pathlib.core.iter_objects.IterObjectsAPIMixin.count_objects` can do that too.

.. code-block:: python

    >>> p = S3Path("bucket", "datalake/")
    >>> p.count_objects(include_folder=False)
    7164 # number of files under this prefix

    >>> p.calculate_total_size(include_folder=False)
    (7164, 236483701963) # 7164 objects, 220.24 GB

    >>> p.calculate_total_size(for_human=True, include_folder=False)
    (7164, '220.24 GB') # 7164 objects, 220.24 GB

.. note::

    In the AWS S3 console, if you clicked "Create Folder" button, it actually creates an empty object with tailing ``/`` to represent the logic folder. It is invisible to human but the empty object actually exists and counts as an object in the native AWS boto3 API.

    As a human we don't care about "logical folder" and want the number we calculate is what we see. So **b default, s3pathlib doesn't count logic folder and also won't yield s3 object in the** :meth:`~s3pathlib.core.iter_objects.IterObjectsAPIMixin.iter_objects` **API**.

    If you insist to see "logical folder", you can use ``include_folder=True`` to enable it.

    You can find more info about "logical folder" in the official doc `Using Folder <https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-folders.html>`_


Select and Filter Objects Under A Prefix
------------------------------------------------------------------------------

Iterate all objects (by default, it doesn't yield "logical folder"):

.. code-block:: python

    p = S3Path("bucket", "datalake/")
    for p_obj in p.iter_objects():
        ...

:meth:`~s3pathlib.core.iter_objects.IterObjectsAPIMixin.iter_objects` also support the following arguments:

- ``batch_size``: number of s3 objects returned per API call, internally it makes pagination API call to iterate through all s3 objects. Large batch size can reduce the total API call and hence inprove performance.
- ``limit``: limit the number of objects you want to return.
- ``recursive``: default is ``True``, it go through sub folder too. But you can set to ``False`` to go through top level folder only
- ``include_folder``: default is ``False``. if ``True``, it also returns empty s3 object ends with tailing ``/``, which is considered as a folder in S3 console.


What's Next
------------------------------------------------------------------------------
``s3pathlib`` aims to make s3 object manipulation as simple as managing local files using ``pathlib``.

let's learn some s3 object manipulation tricks. Go :ref:`stateful-s3-api`
