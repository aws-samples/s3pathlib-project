# -*- coding: utf-8 -*-

from s3pathlib.tests import run_cov_test


class TestBucketAPIMixin:
    def test(self):
        pass


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.core.bucket", preview=False)
