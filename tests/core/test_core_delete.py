# -*- coding: utf-8 -*-

import pytest

from s3pathlib import exc
from s3pathlib.core import S3Path
from s3pathlib.tests import run_cov_test
from s3pathlib.tests.mock import BaseTest


class DeleteAPIMixin(BaseTest):
    module = "core.delete"

    def _test_delete_if_exists(self):
        s3dir_root = self.s3dir_root

        # --- file
        s3path = S3Path(s3dir_root, "delete-if-exists", "test.py")
        s3path.write_text("hello")
        assert s3path.exists() is True

        assert s3path.delete_if_exists() == 1
        assert s3path.exists() is False

        assert s3path.delete_if_exists() == 0
        assert s3path.exists() is False

        # --- dir
        s3dir = S3Path(s3dir_root, "delete-if-exists", "test/")
        s3dir.joinpath("1.txt").write_text("alice")
        s3dir.joinpath("folder/1.txt").write_text("bob")
        assert s3dir.exists() is True

        assert s3dir.delete_if_exists() == 2
        assert s3dir.exists() is False

        assert s3dir.delete_if_exists() == 0
        assert s3dir.exists() is False

    def _test_delete(self):
        s3dir_root = self.s3dir_root

        # --- file
        # prepare
        s3path = S3Path(s3dir_root, "delete-if-exists", "test.txt")
        s3path.write_text("hello")

        # should exist
        assert s3path.exists() is True

        # should not exist
        s3path_new = s3path.delete()
        assert s3path.exists() is False
        assert s3path_new is s3path
        assert s3path_new.version_id == "null"

        # should not exist
        s3path_new = s3path.delete()
        assert s3path.exists() is False
        assert s3path_new.version_id == "null"

        # --- dir
        # prepare
        s3dir = S3Path(s3dir_root, "delete-if-exists", "test/")
        s3dir.joinpath("1.txt").write_text("alice")
        s3dir.joinpath("folder/1.txt").write_text("bob")

        # should exist
        assert s3dir.exists() is True

        # should exist
        s3dir_new = s3dir.delete()
        assert s3dir.exists() is False
        assert s3dir_new is s3dir
        with pytest.raises(exc.S3FileNotExist):
            _ = s3dir_new.version_id

    def _test_delete_with_versioning(self):
        s3dir_root = self.s3dir_root_with_versioning

        # --- file
        s3path = S3Path(s3dir_root, "delete_with_versioning", "test.txt")

        # create the first version
        v1 = s3path.write_text("v1").version_id
        # it should exist
        assert s3path.exists() is True

        # create the second version
        v2 = s3path.write_text("v2").version_id
        # it should exist
        assert s3path.exists() is True

        # delete the latest (second) version
        s3path_new = s3path.delete()
        dmv2 = s3path_new.version_id  # DMV = delete marker version
        # it should not exist
        assert s3path.exists() is False

        # versions are all different
        assert len({v1, v2, dmv2}) == 3

        # all specific versions should exist
        assert s3path.exists(version_id=v1) is True
        assert s3path.exists(version_id=v2) is True

        s3path_new = s3path.delete(version_id=v1)
        assert s3path.exists(version_id=v1) is False

        # --- folder
        # prepare
        s3dir = S3Path(s3dir_root, "delete_with_versioning", "test/")
        s3dir.joinpath("1.txt").write_text("alice")
        s3dir.joinpath("folder/1.txt").write_text("bob")

        # should exist
        assert s3dir.exists() is True

        # should not exist
        s3dir_new = s3dir.delete()
        assert s3dir.exists() is False
        assert s3dir_new is s3dir

    def _test_delete_with_hard_delete(self):
        s3dir_root = self.s3dir_root_with_versioning

        # --- file
        s3path = S3Path(s3dir_root, "delete_with_hard_delete", "test.txt")

        # create the first few versions
        v1 = s3path.write_text("v1").version_id
        v2 = s3path.write_text("v2").version_id

        # delete the object and all versions
        s3path.delete(is_hard_delete=True)
        # folder, object, specific version should not exist
        assert s3path.exists() is False
        assert s3path.exists(version_id=v1) is False
        assert len(s3path.list_object_versions().all()) == 0

        # --- folder
        # prepare
        s3dir = S3Path(s3dir_root, "delete_with_hard_delete", "test/")
        v11 = s3dir.joinpath("1.txt").write_text("v11").version_id
        v12 = s3dir.joinpath("1.txt").write_text("v12").version_id
        v21 = s3dir.joinpath("folder/1.txt").write_text("21").version_id
        v22 = s3dir.joinpath("folder/1.txt").write_text("22").version_id

        # delete the entire folder and all versions
        s3dir.delete(is_hard_delete=True)
        # folder, object, specific version should not exist
        assert s3dir.exists() is False
        assert s3dir.joinpath("1.txt").exists(version_id=v11) is False
        assert s3dir.joinpath("folder/1.txt").exists(version_id=v21) is False
        assert len(s3dir.list_object_versions().all()) == 0

    def test(self):
        with pytest.warns():
            self._test_delete_if_exists()
            
        self._test_delete()

        if self.use_mock is False:  # moto has a bug for deleting object with versioning
            self._test_delete_with_versioning()
            self._test_delete_with_hard_delete()


class Test(DeleteAPIMixin):
    use_mock = False


class TestUseMock(DeleteAPIMixin):
    use_mock = True


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.core.delete", preview=False)
