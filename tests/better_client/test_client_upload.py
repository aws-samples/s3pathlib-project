# -*- coding: utf-8 -*-

import pytest

from s3pathlib.better_client.upload import (
    upload_dir,
)
from s3pathlib.utils import smart_join_s3_key
from s3pathlib.tests import run_cov_test
from s3pathlib.tests.mock import prefix, BaseTest
from s3pathlib.tests.paths import dir_test_upload_dir_folder

dir_test_upload_dir_folder.joinpath("emptyfolder").mkdir(exist_ok=True)

prefix = smart_join_s3_key([prefix, "better_client", "upload"], is_dir=False)


class BetterUpload(BaseTest):
    module = "better_client.upload"

    def _test(self):
        s3_client = self.s3_client
        bucket = self.bucket

        # regular upload
        upload_dir(
            s3_client=s3_client,
            bucket=bucket,
            prefix=smart_join_s3_key(
                parts=[self.prefix, "test_upload_dir"],
                is_dir=True,
            ),
            local_dir=f"{dir_test_upload_dir_folder}",
            pattern="**/*.txt",
            overwrite=True,
        )

        # upload to bucket root directory also works
        upload_dir(
            s3_client=s3_client,
            bucket=bucket,
            prefix="",
            local_dir=f"{dir_test_upload_dir_folder}",
            pattern="**/*.txt",
            overwrite=True,
        )

        # raise error when overwrite is False
        with pytest.raises(FileExistsError):
            upload_dir(
                s3_client=s3_client,
                bucket=bucket,
                prefix=smart_join_s3_key(
                    parts=[self.prefix, "test_upload_dir"],
                    is_dir=True,
                ),
                local_dir=f"{dir_test_upload_dir_folder}",
                pattern="**/*.txt",
                overwrite=False,
            )

        # input argument error
        path = dir_test_upload_dir_folder.joinpath("1.txt")
        with pytest.raises(TypeError):
            upload_dir(
                s3_client=s3_client,
                bucket=bucket,
                prefix="",
                local_dir=f"{path}",
                pattern="**/*.txt",
                overwrite=False,
            )

        dir_not_exist = dir_test_upload_dir_folder.parent.joinpath("not_exist")
        with pytest.raises(FileNotFoundError):
            upload_dir(
                s3_client=s3_client,
                bucket=bucket,
                prefix="",
                local_dir=f"{dir_not_exist}",
                pattern="**/*.txt",
                overwrite=False,
            )

    def test(self):
        self._test()


class Test(BetterUpload):
    use_mock = False


class TestUseMock(BetterUpload):
    use_mock = True


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.better_client.upload", preview=False)
