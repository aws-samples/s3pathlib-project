# -*- coding: utf-8 -*-

import time
from pathlib_mate import Path

from s3pathlib.core import S3Path
from s3pathlib.tests import run_cov_test
from s3pathlib.tests.mock import BaseTest


dir_here = Path.dir_here(__file__)


class IterObjectsAPIMixin(BaseTest):
    module = "core.iter_object_versions"

    def _test_list_object_versions(self):
        # prepare test data
        s3path = S3Path(
            self.s3dir_root_with_versioning, "list_object_versions", "file.txt"
        )
        # clear existing data
        s3path.delete(is_hard_delete=True)

        v1 = s3path.write_text("v1").version_id
        time.sleep(1)
        s3path.delete()
        time.sleep(1)
        v2 = s3path.write_text("v2").version_id
        time.sleep(1)
        v3 = s3path.write_text("v3").version_id

        s3path_list = s3path.list_object_versions().all()
        versions = [s3path.version_id for s3path in s3path_list]
        assert versions[0] == v3
        assert versions[1] == v2
        assert versions[3] == v1

        assert [s3path.is_delete_marker() for s3path in s3path_list] == [
            False, # v3
            False, # v2
            True, # delete marker
            False # v1
        ]
        assert s3path_list[2].etag is None
        assert s3path_list[2].size == 0


    def test(self):
        self._test_list_object_versions()


class Test(IterObjectsAPIMixin):
    use_mock = False


class TestUseMock(IterObjectsAPIMixin):
    use_mock = True


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.core.iter_object_versions", preview=False)
