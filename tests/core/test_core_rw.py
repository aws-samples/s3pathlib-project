# -*- coding: utf-8 -*-

import pytest
from s3pathlib import exc
from s3pathlib.core import S3Path
from s3pathlib.tests import run_cov_test
from s3pathlib.tests.mock import BaseTest


class ReadAndWriteAPIMixin(BaseTest):
    module = "core.rw"

    def _test_text_bytes_io(self):
        s3dir_root = self.s3dir_root

        # text
        s = "this is text"
        p = S3Path(s3dir_root, "write", "file.txt")
        p.clear_cache()

        p_new = p.write_text(
            s,
            metadata={"file-type": "txt"},
            tags={"key1": "value1", "key2": "alice=bob"},
        )
        assert p_new.read_text() == s
        assert p_new.metadata == {"file-type": "txt"}
        assert p_new.get_tags()[1] == {"key1": "value1", "key2": "alice=bob"}
        assert p_new.size == len(s)
        assert p_new.etag is not None
        assert p_new.last_modified_at is not None
        assert p_new.version_id == "null"

        # bytes
        b = "this is bytes".encode("utf-8")
        p = S3Path(s3dir_root, "write", "file.dat")
        p.clear_cache()

        p_new = p.write_bytes(
            b,
            metadata={"file-type": "binary"},
            tags={"key1": "value1", "key2": "alice=bob"},
        )
        assert p_new.read_bytes() == b
        assert p_new.metadata == {"file-type": "binary"}
        assert p_new.get_tags()[1] == {"key1": "value1", "key2": "alice=bob"}
        assert p_new.size == len(b)
        assert p_new.etag is not None
        assert p_new.last_modified_at is not None
        assert p_new.version_id == "null"

    def _test_text_bytes_io_with_metadata_and_tags(self):
        p = S3Path(self.s3dir_root, "write_with_metadata_and_tags", "hello.txt")
        p.delete_if_exists()
        p.clear_cache()

        # --- put object without metadata and tags
        p.write_text("hello")

        # size is not 0
        assert p.size != 0

        # metadata is empty
        assert p.metadata == {}

        # tags is empty
        assert p.get_tags()[1] == {}

        # --- use put_object API
        p.write_text(
            "",
            metadata={"file-type": "txt"},
            tags={"k1": "v1"},
        )
        p.clear_cache()

        assert p.size == 0  # content erased, because we just put_object without body
        assert p.metadata == {"file-type": "txt"}
        assert p.get_tags()[1] == {"k1": "v1"}

        # --- use put_object API again
        p.write_text(
            "", metadata={"creator": "s3pathlib"}, tags={"k2": "v2", "k3": "v3"}
        )
        p.clear_cache()

        # put object tagging is a full replacement
        assert p.metadata == {"creator": "s3pathlib"}
        assert p.get_tags()[1] == {
            "k2": "v2",
            "k3": "v3",
        }

    def _test_touch(self):
        s3dir_root = self.s3dir_root

        p = S3Path(s3dir_root, "touch", "test.txt")
        p.delete_if_exists()

        assert p.exists() is False
        p.touch()
        assert p.exists() is True
        assert p.size == 0
        with pytest.raises(exc.S3ObjectAlreadyExist):
            p.touch(exist_ok=False)
        p.touch()

        p = S3Path(s3dir_root, "touch", "folder/")
        with pytest.raises(TypeError):
            p.touch()

    def _test_mkdir(self):
        s3dir_root = self.s3dir_root

        # case 1, exists_ok = False, parents = True
        p_root = S3Path(s3dir_root, "mkdir/")

        # clear off all existing folders
        p_root.delete_if_exists()

        p_f3 = S3Path(p_root, "f1", "f2", "f3/")
        p_f2 = p_f3.parent
        p_f1 = p_f2.parent

        # at begin, all folders not exist
        assert p_f3.exists() is False
        assert p_f2.exists() is False
        assert p_f1.exists() is False
        assert p_root.count_objects(include_folder=True) == 0

        # invoke api
        p_f3.mkdir(parents=True)

        assert p_root.count_objects(include_folder=True) == 4

        assert p_f3.exists() is True
        assert p_f2.exists() is True
        assert p_f1.exists() is True

        assert p_f3.count_objects(include_folder=False) == 0
        assert p_f2.count_objects(include_folder=False) == 0
        assert p_f1.count_objects(include_folder=False) == 0

        # case 2
        # by default, it doesn't allow make dir if already exists
        with pytest.raises(exc.S3FolderAlreadyExist):
            p_f3.mkdir()

        p_f3.mkdir(exist_ok=True)

        with pytest.raises(exc.S3PathIsNotFolderError):
            S3Path(p_root, "test.txt").mkdir()

    def _test_with_versioning(self):
        s3path = S3Path(self.s3dir_root_with_versioning, "with_versioning", "test.txt")
        if self.use_mock:
            with pytest.raises(exc.S3ObjectNotExist):
                _ = s3path.version_id

        s3path_new = s3path.write_text("v1")
        v1 = s3path_new.version_id
        assert v1 != "null"
        assert s3path_new.is_delete_marker() is False
        assert s3path_new.size == 2
        assert s3path_new.etag is not None
        assert s3path_new.last_modified_at is not None

        s3path_new = s3path.write_text("v22")
        v2 = s3path_new.version_id
        assert v2 != "null"
        assert v2 != v1
        assert s3path_new.size == 3
        assert s3path_new.etag is not None
        assert s3path_new.last_modified_at is not None
        assert s3path_new.is_delete_marker() is False

    def test(self):
        self._test_text_bytes_io()
        self._test_text_bytes_io_with_metadata_and_tags()
        self._test_touch()
        self._test_mkdir()
        self._test_with_versioning()


class Test(ReadAndWriteAPIMixin):
    use_mock = False


class TestUseMock(ReadAndWriteAPIMixin):
    use_mock = True


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.core.rw", preview=False)
