# -*- coding: utf-8 -*-

import pytest

from s3pathlib.core import S3Path
from s3pathlib.tests import run_cov_test


class TestIsTestAPIMixin:
    def test_type_test(self):
        """
        Test if the instance is a ...

        - is_dir()
        - is_file()
        - is_bucket()
        - is_void()
        """
        # s3 object
        p = S3Path("bucket", "file.txt")
        assert p.is_void() is False
        assert p.is_dir() is False
        assert p.is_file() is True
        assert p.is_bucket() is False
        p.ensure_file()
        with pytest.raises(Exception):
            p.ensure_not_file()
        with pytest.raises(Exception):
            p.ensure_dir()
        p.ensure_not_dir()

        # s3 directory
        p = S3Path("bucket", "folder/")
        assert p.is_void() is False
        assert p.is_dir() is True
        assert p.is_file() is False
        assert p.is_bucket() is False
        with pytest.raises(Exception):
            p.ensure_file()
        p.ensure_not_file()
        p.ensure_dir()
        with pytest.raises(Exception):
            p.ensure_not_dir()

        # s3 bucket
        p = S3Path("bucket")
        assert p.is_void() is False
        assert p.is_dir() is True
        assert p.is_file() is False
        assert p.is_bucket() is True

        # void path
        p = S3Path()
        assert p.is_void() is True
        assert p.is_dir() is False
        assert p.is_file() is False
        assert p.is_bucket() is False


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.core.is_test", preview=False)
