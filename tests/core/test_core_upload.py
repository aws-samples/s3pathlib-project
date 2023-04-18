# -*- coding: utf-8 -*-

import pytest
from pathlib_mate import Path
from s3pathlib.core import S3Path
from s3pathlib.tests import run_cov_test
from s3pathlib.tests.mock import BaseTest


dir_here = Path.dir_here(__file__)


class UploadAPIMixin(BaseTest):
    module = "core.upload"

    def _test_upload_file(self):
        # before state
        p = S3Path(self.s3dir_root, "upload-file", "test.py")
        p.delete_if_exists()
        assert p.exists() is False

        # invoke api
        p.upload_file(path=__file__, overwrite=True)

        # after state
        assert p.exists() is True

        # raise exception if exists
        with pytest.raises(FileExistsError):
            p.upload_file(path=__file__, overwrite=False)

        # raise type error if upload to a folder
        with pytest.raises(TypeError):
            p = S3Path("bucket", "folder/")
            p.upload_file("/tmp/file.txt")

    def _test_upload_dir(self):
        # before state
        p = S3Path(self.s3dir_root, "upload-dir/")
        p.delete_if_exists()
        assert p.count_objects() == 0

        # invoke api
        dir_to_upload = dir_here.joinpath("test_upload_dir").abspath
        p.upload_dir(
            local_dir=dir_to_upload,
            pattern="**/*.txt",
            overwrite=True,
        )

        # after state
        assert p.count_objects() == 2

        # raise exception if exists
        with pytest.raises(FileExistsError):
            p.upload_dir(
                local_dir=dir_to_upload,
                pattern="**/*.txt",
                overwrite=False,
            )

        # raise type error if upload to a folder
        with pytest.raises(TypeError):
            p = S3Path("bucket", "file.txt")
            p.upload_dir("/tmp/folder")

    def test(self):
        self._test_upload_file()
        self._test_upload_dir()


class Test(UploadAPIMixin):
    use_mock = False


class TestUseMock(UploadAPIMixin):
    use_mock = True


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.core.upload", preview=False)
