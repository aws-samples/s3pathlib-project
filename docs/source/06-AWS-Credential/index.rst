.. _aws-credential:

AWS Credential
==============================================================================
.. contents::
    :class: this-will-duplicate-information-and-it-is-still-useful-here
    :depth: 1
    :local:


By default, s3pathlib uses a singleton object :ref:`context <configure-aws-context>`, to manage the underlying boto session. However, you still have more grained control.

Every :class:`~s3pathlib.core.s3path.S3Path` methods that requires a boto session has an optional argument: ``bsm``. It is a ``BotoSesManager`` object from `boto_session_manager <https://pypi.org/project/boto-session-manager/>`_ library. You can explicitly define what AWS credential you want to use for the API call.

.. code-block:: python

    from s3pathlib import S3Path
    from boto_session_manager import BotoSesManager, AwsServiceEnum

    bsm = BotoSesManager(profile_name="my_profile")

    s3path = S3Path.from_s3_uri("s3://my-bucket/my-file.txt")
    s3path.exists(bsm)
