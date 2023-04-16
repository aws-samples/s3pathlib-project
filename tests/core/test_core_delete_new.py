# -*- coding: utf-8 -*-

from s3pathlib.core import S3Path
from s3pathlib.tests import bucket, prefix, run_cov_test

s3dir_root = S3Path(bucket, prefix, "core", "delete").to_dir()


class TestDeleteAPIMixin:
    def test_delete_if_exists(self):
        # file
        s3path = S3Path(s3dir_root, "delete-if-exists", "test.py")
        s3path.write_text("hello")
        assert s3path.delete_if_exists() == 1
        assert s3path.delete_if_exists() == 0

        # dir
        s3dir = S3Path(s3dir_root, "delete-if-exists", "test/")
        s3dir.joinpath("1.txt").write_text("alice")
        s3dir.joinpath("folder/1.txt").write_text("bob")
        assert s3dir.delete_if_exists() == 2
        assert s3dir.delete_if_exists() == 0


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.core.delete", preview=False)
