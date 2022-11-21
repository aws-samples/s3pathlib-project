S3 Object Metadata and Tagging
==============================================================================
Manage S3 object metadata and tagging is a common use case. Let's see how it's done with ``s3pathlib``:

.. code-block:: python

    from s3pathlib import S3Path

    s3path = S3Path()
    s3path.write_text(
        "hello",
        metadata={"creator": "s3pathlib"},
        tags={"description": "hello 1"},
    )
    print(s3path.metadata) # {"creator": "s3pathlib"}
    print(s3path.get_tags()) # {"description": "hello 1"}

    s3path.put_tags({"description": "hello 2"})
    print(s3path.get_tags()) # {"description": "hello 2"}

    s3path.update_tags({"owner": "s3pathlib"})
    print(s3path.get_tags()) # {"description": "hello 2", "owner": "s3pathlib"}

You can also use the following API to manipulate metadata and taggings.

- :meth:`~s3pathlib.core.rw.ReadAndWriteAPIMixin.write_bytes`
- :meth:`~s3pathlib.core.rw.ReadAndWriteAPIMixin.write_text`
- :meth:`~s3pathlib.core.rw.ReadAndWriteAPIMixin.touch`
- :meth:`~s3pathlib.core.opener.OpenerAPIMixin.open`
- :meth:`~s3pathlib.core.tagging.TaggingAPIMixin.get_tags`
- :meth:`~s3pathlib.core.tagging.TaggingAPIMixin.put_tags`
- :meth:`~s3pathlib.core.tagging.TaggingAPIMixin.update_tags`

Note

    Once the user defined metadata is written to S3 object, you cannot just change metadata without writing the content. Metadata and the content has to updated all together in one API call.
