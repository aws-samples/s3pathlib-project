# -*- coding: utf-8 -*-

from s3pathlib.core import S3Path
from s3pathlib.tests import run_cov_test
from s3pathlib.tests.mock import BaseTest


class DeleteAPIMixin(BaseTest):
    module = "core.delete"

    def _test_delete_if_exists(self):
        s3dir_root = self.s3dir_root

        # --- file
        s3path = S3Path(s3dir_root, "delete-if-exists", "test.py")
        s3path.write_text("hello")
        assert s3path.exists() is True

        assert s3path.delete_if_exists() == 1
        assert s3path.exists() is False

        assert s3path.delete_if_exists() == 0
        assert s3path.exists() is False

        # --- dir
        s3dir = S3Path(s3dir_root, "delete-if-exists", "test/")
        s3dir.joinpath("1.txt").write_text("alice")
        s3dir.joinpath("folder/1.txt").write_text("bob")
        assert s3dir.exists() is True

        assert s3dir.delete_if_exists() == 2
        assert s3dir.exists() is False

        assert s3dir.delete_if_exists() == 0
        assert s3dir.exists() is False

    def test(self):
        self._test_delete_if_exists()


class Test(DeleteAPIMixin):
    use_mock = False


class TestUseMock(DeleteAPIMixin):
    use_mock = True


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.core.delete", preview=False)
