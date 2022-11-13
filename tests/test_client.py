# -*- coding: utf-8 -*-

from s3pathlib import S3Path
from s3pathlib.client import put_object
from s3pathlib.tests import s3_client, bucket, prefix


class TestClient:
    p_root = S3Path(bucket, prefix, "client")

    def test_put_object(self):
        p = S3Path(self.p_root, "put_object", "hello.txt")
        put_object(
            s3_client,
            p.bucket,
            p.key,
            b"hello",
            metadata={"file-type": "txt"},
            tags={"creator": "s3pathlib"},
        )


if __name__ == "__main__":
    from s3pathlib.tests import run_cov_test

    run_cov_test(__file__, module="s3pathlib.client", open_browser=False)
