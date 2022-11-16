# -*- coding: utf-8 -*-

import pytest
from s3pathlib.core import S3Path
from s3pathlib import client as better_client
from s3pathlib.tests import s3_client, bucket, prefix, run_cov_test

s3dir_root = S3Path(bucket, prefix, "core", "exists/")


class TestExistsAPIMixin:
    def test_exists(self):
        # s3 bucket
        assert S3Path(bucket).exists() is True
        # have access but not exists
        assert S3Path(f"{bucket}-not-exists").exists() is False
        # doesn't have access
        with pytest.raises(Exception):
            assert S3Path("asdf").exists() is False

        # s3 object
        s3path_file = s3dir_root.joinpath("file.txt")
        better_client.put_object(
            s3_client,
            s3path_file.bucket,
            s3path_file.key,
            b"a",
        )
        assert s3path_file.exists() is True

        s3path_empty_object = s3dir_root.joinpath("empty.txt")
        better_client.put_object(
            s3_client,
            s3path_empty_object.bucket,
            s3path_empty_object.key,
            b"",
        )
        assert s3path_empty_object.exists() is True

        assert S3Path(bucket, "this-never-gonna-exists.exe").exists() is False

        # s3 directory
        assert s3path_file.parent.exists() is True

        assert S3Path(bucket, "this-never-gonna-exists/").exists() is False

        # void path
        p = S3Path()
        with pytest.raises(TypeError):
            p.exists()

        # relative path
        p = S3Path.make_relpath("folder", "file.txt")
        with pytest.raises(TypeError):
            p.exists()

        # soft folder
        s3path_soft_folder_file = s3dir_root.joinpath("soft_folder", "file.txt")
        better_client.put_object(
            s3_client,
            s3path_soft_folder_file.bucket,
            s3path_soft_folder_file.key,
            b"a",
        )
        assert s3path_soft_folder_file.parent.exists() is True
        assert s3path_soft_folder_file.exists() is True

        # hard folder
        dir_hard_folder = s3dir_root.joinpath("hard_folder/")
        better_client.put_object(
            s3_client,
            dir_hard_folder.bucket,
            dir_hard_folder.key,
            b"",
        )
        s3path_hard_folder_file = s3dir_root.joinpath("hard_folder", "file.txt")
        better_client.put_object(
            s3_client,
            s3path_hard_folder_file.bucket,
            s3path_hard_folder_file.key,
            b"a",
        )
        assert dir_hard_folder.exists() is True
        assert s3path_hard_folder_file.exists() is True

        # empty folder
        dir_empty_folder = s3dir_root.joinpath("empty_folder/")
        better_client.put_object(
            s3_client,
            dir_empty_folder.bucket,
            dir_empty_folder.key,
            b"",
        )
        assert dir_empty_folder.exists() is True


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.core.exists", open_browser=False)
