# -*- coding: utf-8 -*-

import pytest
from s3pathlib.core import S3Path
from s3pathlib.tests import run_cov_test
from s3pathlib.tests.mock import BaseTest


class ExistsAPIMixin(BaseTest):
    module = "core.exists"

    def _test_exists(self):
        # s3 bucket
        assert S3Path(self.bucket).exists() is True

        # have access but not exists
        assert S3Path(f"{self.bucket}-not-exists").exists() is False

        # doesn't have access
        if self.use_mock is False:
            with pytest.raises(Exception):
                assert S3Path("asdf").exists() is False

        # s3 object
        s3path_file = self.s3dir_root.joinpath("file.txt")
        self.s3_client.put_object(
            Bucket=s3path_file.bucket,
            Key=s3path_file.key,
            Body=b"a",
        )
        assert s3path_file.exists() is True

        with pytest.raises(Exception):
            s3path_file.ensure_not_exists()

        s3path_empty_object = self.s3dir_root.joinpath("empty.txt")
        self.s3_client.put_object(
            Bucket=s3path_empty_object.bucket,
            Key=s3path_empty_object.key,
            Body=b"",
        )
        assert s3path_empty_object.exists() is True

        assert S3Path(self.bucket, "this-never-gonna-exists.exe").exists() is False

        # s3 directory
        assert s3path_file.parent.exists() is True

        assert S3Path(self.bucket, "this-never-gonna-exists/").exists() is False

        # void path
        p = S3Path()
        with pytest.raises(TypeError):
            p.exists()

        # relative path
        p = S3Path.make_relpath("folder", "file.txt")
        with pytest.raises(TypeError):
            p.exists()

        # soft folder
        s3path_soft_folder_file = self.s3dir_root.joinpath("soft_folder", "file.txt")
        self.s3_client.put_object(
            Bucket=s3path_soft_folder_file.bucket,
            Key=s3path_soft_folder_file.key,
            Body=b"a",
        )
        assert s3path_soft_folder_file.parent.exists() is True
        assert s3path_soft_folder_file.exists() is True

        # hard folder
        dir_hard_folder = self.s3dir_root.joinpath("hard_folder/")
        self.s3_client.put_object(
            Bucket=dir_hard_folder.bucket,
            Key=dir_hard_folder.key,
            Body=b"",
        )
        s3path_hard_folder_file = self.s3dir_root.joinpath("hard_folder", "file.txt")
        self.s3_client.put_object(
            Bucket=s3path_hard_folder_file.bucket,
            Key=s3path_hard_folder_file.key,
            Body=b"a",
        )
        assert dir_hard_folder.exists() is True
        assert s3path_hard_folder_file.exists() is True

        # empty folder
        dir_empty_folder = self.s3dir_root.joinpath("empty_folder/")
        self.s3_client.put_object(
            Bucket=dir_empty_folder.bucket,
            Key=dir_empty_folder.key,
            Body=b"",
        )
        assert dir_empty_folder.exists() is True

    def test(self):
        self._test_exists()


class Test(ExistsAPIMixin):
    use_mock = False


class TestUseMock(ExistsAPIMixin):
    use_mock = True


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.core.exists", preview=False)
