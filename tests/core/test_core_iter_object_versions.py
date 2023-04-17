# -*- coding: utf-8 -*-

import pytest

from pathlib_mate import Path
from iterproxy import and_

from s3pathlib.core import S3Path
from s3pathlib.client import put_object
from s3pathlib.tests import run_cov_test
from s3pathlib.tests.mock import BaseTest


dir_here = Path.dir_here(__file__)


class IterObjectsAPIMixin(BaseTest):
    module = "core.iter_objects"

    # @classmethod
    # def custom_setup_class(cls):
    #     s3dir_root = cls.get_s3dir_root()
    #
    #     s3dir_hard_empty_folder = S3Path(s3dir_root, "hard_empty_folder").to_dir()
    #     s3dir_hard_empty_folder.mkdir()
    #
    #     s3dir_hard_folder = S3Path(s3dir_root, "hard_folder").to_dir()
    #     s3dir_hard_folder.mkdir()
    #
    #     s3dir_soft_folder = S3Path(s3dir_root, "soft_folder").to_dir()
    #     s3dir_soft_folder_file = S3Path(s3dir_soft_folder, "file.txt")
    #     s3dir_soft_folder_file.write_text("content v1")
    #     s3dir_soft_folder_file.write_text("content v2")
    #
    #     s3path_log = S3Path(s3dir_root, "log.txt").to_dir()
    #     s3path_log.write_text("content v1")
    #     s3path_log.write_text("content v2")
    #     s3path_log.delete_if_exists()


    def test(self):
        pass


# class Test(IterObjectsAPIMixin):
#     use_mock = False


class TestUseMock(IterObjectsAPIMixin):
    use_mock = True


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.core.iter_object_versions", preview=False)
