# -*- coding: utf-8 -*-

import pytest
from pathlib_mate import Path
from s3pathlib import client as better_client
from s3pathlib.core import S3Path
from s3pathlib.tests import s3_client, bucket, prefix, run_cov_test


dir_here = Path.dir_here(__file__)

s3dir_root = S3Path(bucket, prefix, "core", "upload").to_dir()


class TestUploadAPIMixin:
    def test_upload_file(self):
        # before state
        p = S3Path(s3dir_root, "upload-file", "test.py")
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

    def test_upload_dir(self):
        # before state
        p = S3Path(s3dir_root, "upload-dir/")
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


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.core.upload", open_browser=False)
