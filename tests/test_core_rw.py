# -*- coding: utf-8 -*-

import pytest
from s3pathlib.aws import context
from s3pathlib.core import S3Path
from s3pathlib.tests import boto_ses, bucket, prefix, run_cov_test

context.attach_boto_session(boto_ses)


class TestS3Path:
    p_root = S3Path(bucket, prefix, "io")

    def test_text_bytes_io(self):
        # text
        s = "this is text"
        p = S3Path(self.p_root, "file.txt")
        p.clear_cache()

        p.write_text(
            s,
            metadata={"file-type": "txt"},
            tags={"key1": "value1", "key2": "alice=bob"},
        )
        assert p.read_text() == s
        assert p.metadata == {"file-type": "txt"}
        assert p.get_tags() == {"key1": "value1", "key2": "alice=bob"}

        # bytes
        b = "this is bytes".encode("utf-8")
        p = S3Path(self.p_root, "file.dat")
        p.clear_cache()

        p.write_bytes(
            b,
            metadata={"file-type": "binary"},
            tags={"key1": "value1", "key2": "alice=bob"},
        )
        assert p.read_bytes() == b
        assert p.metadata == {"file-type": "binary"}
        assert p.get_tags() == {"key1": "value1", "key2": "alice=bob"}

    def test_metadata_tags(self):
        p = S3Path(self.p_root, "log.txt")

        # put metadata and tags
        p.write_text(
            "hello",
            metadata={"creator": "s3pathlib"},
            tags={"creator": "s3pathlib"},
        )
        assert p.metadata == {"creator": "s3pathlib"}
        assert p.get_tags() == {"creator": "s3pathlib"}

        # put content without metadata and tags
        p.write_text("hello")
        p.head_object()
        assert p.metadata == {}
        assert p.get_tags() == {}



if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.core.rw", open_browser=False)
