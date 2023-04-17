# -*- coding: utf-8 -*-

import pytest

from s3pathlib.core import S3Path
from s3pathlib import exc
from s3pathlib.better_client.upload import (
    upload_dir,
)
from s3pathlib.utils import smart_join_s3_key
from s3pathlib.tests import run_cov_test
from s3pathlib.tests.mock import prefix, BaseTest

from dummy_data import DummyData


class BetterUpload(BaseTest):
    module = "better_client.upload"

    def _test(self):
        s3_client = self.s3_client
        bucket = self.bucket

        local_dir = os.path.join(dir_tests, "core", "test_upload_dir")

        # regular upload
        upload_dir(
            s3_client=s3_client,
            bucket=bucket,
            prefix=smart_join_s3_key(
                parts=[prefix, "test_upload_dir"],
                is_dir=True,
            ),
            local_dir=local_dir,
            pattern="**/*.txt",
            overwrite=True,
        )

        # upload to bucket root directory also works
        upload_dir(
            s3_client=s3_client,
            bucket=bucket,
            prefix="",
            local_dir=local_dir,
            pattern="**/*.txt",
            overwrite=True,
        )

        with pytest.raises(FileExistsError):
            upload_dir(
                s3_client=s3_client,
                bucket=bucket,
                prefix=smart_join_s3_key(
                    parts=[prefix, "test_upload_dir"],
                    is_dir=True,
                ),
                local_dir=local_dir,
                pattern="**/*.txt",
                overwrite=False,
            )

    def test(self):
        self._test()


# class Test(BetterUpload):
#     use_mock = False


class TestUseMock(BetterUpload):
    use_mock = True


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.better_client.upload", preview=False)
