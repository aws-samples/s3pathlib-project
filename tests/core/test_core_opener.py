# -*- coding: utf-8 -*-

import json
import pickle
from s3pathlib.core import S3Path
from s3pathlib.tests import bucket, prefix, run_cov_test

s3dir_root = S3Path(bucket, prefix, "core", "open").to_dir()


class TestOpenerAPIMixin:
    def test_open(self):
        s3path = s3dir_root.joinpath("data.json")
        with s3path.open("w") as f:
            json.dump({"a": 1}, f)
        with s3path.open("r") as f:
            assert json.load(f) == {"a": 1}

        s3path = s3dir_root.joinpath("data.pickle")
        with s3path.open("wb") as f:
            pickle.dump({"a": 1}, f)
        with s3path.open("rb") as f:
            assert pickle.load(f) == {"a": 1}


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.core.opener", open_browser=False)
