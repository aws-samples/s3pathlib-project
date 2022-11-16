# -*- coding: utf-8 -*-

from s3pathlib.core import S3Path
from s3pathlib.tests import run_cov_test


class TestSerdeAPIMixin:
    def test_serialization(self):
        p1 = S3Path("bucket", "folder", "file.txt")
        assert p1.to_dict() == {
            "bucket": "bucket",
            "parts": ["folder", "file.txt"],
            "is_dir": False,
        }
        p2 = S3Path.from_dict(p1.to_dict())
        assert p1 == p2
        assert p1 is not p2


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.core.serde", open_browser=False)
