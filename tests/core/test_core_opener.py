# -*- coding: utf-8 -*-

import json
import pickle
from s3pathlib.core import S3Path
from s3pathlib.tests import run_cov_test
from s3pathlib.tests.mock import BaseTest


class OpenerAPIMixin(BaseTest):
    module = "core.open"

    def _test_open(self):
        s3path = S3Path(self.s3dir_root, "data.json")
        with s3path.open("w") as f:
            json.dump({"a": 1}, f)
        with s3path.open("r") as f:
            assert json.load(f) == {"a": 1}

        s3path = S3Path(self.s3dir_root, "data.pickle")
        with s3path.open("wb") as f:
            pickle.dump({"a": 1}, f)
        with s3path.open("rb") as f:
            assert pickle.load(f) == {"a": 1}

    def _test_open_with_additional_kwargs(self):
        s3path = S3Path(self.s3dir_root, "log.txt")

        # multi part upload
        s3path.delete_if_exists()
        with s3path.open(
            "w",
            multipart_upload=True,
            metadata={"creator": "s3pathlib"},
            tags={"project": "s3pathlib"},
        ) as f:
            f.write("hello")

        assert s3path.metadata == {"creator": "s3pathlib"}
        assert s3path.get_tags() == {"project": "s3pathlib"}

        # normal upload
        s3path.delete_if_exists()
        with s3path.open(
            "w",
            multipart_upload=False,
            metadata={"creator": "s3pathlib"},
            tags={"project": "s3pathlib"},
        ) as f:
            f.write("hello")

        assert s3path.metadata == {"creator": "s3pathlib"}
        assert s3path.get_tags() == {"project": "s3pathlib"}

    def test(self):
        self._test_open()
        self._test_open_with_additional_kwargs()


class Test(OpenerAPIMixin):
    use_mock = False


class TestUseMock(OpenerAPIMixin):
    use_mock = True


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.core.opener", preview=False)
