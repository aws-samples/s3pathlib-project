# -*- coding: utf-8 -*-

import pytest
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

        p.write_text(
            s,
            metadata={"file-type": "txt"},
            tags={"key1": "value1", "key2": "alice=bob"},
        )
        assert p.read_text() == s
        assert p.metadata == {"file-type": "txt"}
        assert p.get_tags()[1] == {"key1": "value1", "key2": "alice=bob"}

        # bytes
        b = "this is bytes".encode("utf-8")
        p = S3Path(s3dir_root, "write", "file.dat")
        p.clear_cache()

        p.write_bytes(
            b,
            metadata={"file-type": "binary"},
            tags={"key1": "value1", "key2": "alice=bob"},
        )
        assert p.read_bytes() == b
        assert p.metadata == {"file-type": "binary"}
        assert p.get_tags()[1] == {"key1": "value1", "key2": "alice=bob"}

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
        with pytest.raises(FileExistsError):
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
        with pytest.raises(FileExistsError):
            p_f3.mkdir()

        p_f3.mkdir(exist_ok=True)

        with pytest.raises(ValueError):
            S3Path(p_root, "test.txt").mkdir()

    def test(self):
        self._test_text_bytes_io()
        self._test_text_bytes_io_with_metadata_and_tags()
        self._test_touch()
        self._test_mkdir()


class Test(ReadAndWriteAPIMixin):
    use_mock = False


class TestUseMock(ReadAndWriteAPIMixin):
    use_mock = True


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.core.rw", preview=False)
