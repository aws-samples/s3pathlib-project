# -*- coding: utf-8 -*-

import pytest
from s3pathlib.aws import context
from s3pathlib.core import S3Path
from s3pathlib.tests import boto_ses, bucket, prefix

context.attach_boto_session(boto_ses)


class TestS3Path:
    p_root = S3Path(bucket, prefix, "io")

    def test_text_bytes_io(self):
        s = "this is text"
        p = S3Path(self.p_root, "file.txt")
        p.write_text(s)
        assert p.read_text() == s

        b = "this is bytes".encode("utf-8")
        p = S3Path(self.p_root, "file.dat")
        p.write_bytes(b)
        assert p.read_bytes() == b


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
