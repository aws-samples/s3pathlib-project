# -*- coding: utf-8 -*-

import sys
import pytest
from pathlib_mate import Path
from s3pathlib.core import S3Path
from s3pathlib.tests import run_cov_test
from s3pathlib.tests.mock import BaseTest


dir_here = Path.dir_here(__file__)


class SyncAPIMixin(BaseTest):
    module = "core.sync"

    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="windows CLI system is different",
    )
    def _test_sync(self):
        s3path1 = self.s3dir_root.joinpath("dir1").to_dir()
        s3path2 = self.s3dir_root.joinpath("dir2").to_dir()
        path1 = dir_here.joinpath("dir1")
        path2 = dir_here.joinpath("dir2")
        path0 = dir_here.joinpath("test_upload_dir")

        s3path1.delete_if_exists()
        s3path2.delete_if_exists()
        path1.remove_if_exists()
        path2.remove_if_exists()

        s3path1.sync_from(path0, verbose=False)
        assert s3path1.count_objects() == 2

        s3path1.sync_to(s3path2, verbose=False)
        assert s3path2.count_objects() == 2

        s3path1.sync_to(s3path2.uri, verbose=False)
        assert s3path2.count_objects() == 2

        s3path1.sync_from(s3path2.uri, verbose=False)
        assert s3path1.count_objects() == 2

        s3path2.sync_to(path1, verbose=False)
        assert path1.file_stat()["file"] == 2

        with pytest.raises(ValueError):
            S3Path.sync(path1.abspath, path2.abspath, verbose=False)

    def test(self):
        if self.use_mock is False:
            self._test_sync()


# NOTE: this module should ONLY be tested with MOCK
# DO NOT USE REAL S3 BUCKET
class Test(SyncAPIMixin):
    use_mock = False


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.core.sync", preview=False)
