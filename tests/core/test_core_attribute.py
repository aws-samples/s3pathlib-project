# -*- coding: utf-8 -*-

import pytest
from s3pathlib.core import S3Path
from s3pathlib.tests import run_cov_test
from s3pathlib.tests.mock import BaseTest


class AttributeAPIMixin(BaseTest):
    def _test_properties(self):
        # s3 object
        p = S3Path("bucket", "folder", "file.txt")
        assert str(p) == "S3Path('s3://bucket/folder/file.txt')"
        assert p.basename == "file.txt"
        assert p.fname == "file"
        assert p.ext == ".txt"
        assert p.dirname == "folder"
        assert p.abspath == "/folder/file.txt"
        assert p.dirpath == "/folder/"

        # s3 directory
        p = S3Path("bucket", "folder/")
        assert str(p) == "S3Path('s3://bucket/folder/')"
        assert p.basename == "folder"
        with pytest.raises(TypeError):
            assert p.fname == "folder"
        with pytest.raises(TypeError):
            assert p.ext == ".txt"
        assert p.dirname == ""
        assert p.abspath == "/folder/"
        assert p.dirpath == "/"

        # s3 bucket
        p = S3Path("bucket")
        assert str(p) == "S3Path('s3://bucket/')"
        assert p.basename == ""
        with pytest.raises(TypeError):
            _ = p.fname
        with pytest.raises(TypeError):
            _ = p.ext
        assert p.dirname == ""
        assert p.abspath == "/"
        assert p.dirpath == "/"

        # void path
        p = S3Path()
        assert str(p) == "S3VoidPath()"
        assert p.basename == ""
        with pytest.raises(ValueError):
            _ = p.fname
        with pytest.raises(ValueError):
            _ = p.ext
        assert p.dirname == ""
        with pytest.raises(TypeError):
            _ = p.abspath
        with pytest.raises(TypeError):
            _ = p.abspath

        # relative path
        p = S3Path("bucket/folder/file.txt").relative_to(S3Path("bucket"))
        assert str(p) == "S3RelPath('folder/file.txt')"
        assert p.basename == "file.txt"
        assert p.fname == "file"
        assert p.ext == ".txt"
        assert p.dirname == "folder"
        with pytest.raises(TypeError):
            _ = p.abspath
        with pytest.raises(TypeError):
            _ = p.abspath

    def _test_parent(self):
        p = S3Path("bucket", "folder", "file.txt").parent
        assert p._bucket == "bucket"
        assert p._parts == ["folder"]
        assert p._is_dir is True

        p = S3Path("bucket", "folder", "subfolder/").parent
        assert p._bucket == "bucket"
        assert p._parts == ["folder"]
        assert p._is_dir is True

        p = S3Path("bucket", "folder").parent
        assert p._bucket == "bucket"
        assert p._parts == []
        assert p._is_dir is True

        p = S3Path("bucket", "file.txt").parent
        assert p._bucket == "bucket"
        assert p._parts == []
        assert p._is_dir is True

        p = S3Path("bucket").parent
        assert p._bucket == "bucket"
        assert p._parts == []
        assert p._is_dir is True

        p = S3Path().parent
        assert p._bucket == None
        assert p._parts == []
        assert p._is_dir is None

    def _test_parents(self):
        p_list = S3Path("bucket", "folder", "file.txt").parents
        assert p_list[0].uri == "s3://bucket/folder/"
        assert len(p_list) == 2

        p_list = S3Path("bucket", "folder", "subfolder/").parents
        assert p_list[0].uri == "s3://bucket/folder/"
        assert len(p_list) == 2

        p_list = S3Path("bucket", "folder").parents
        assert len(p_list) == 1

        p_list = S3Path("bucket", "file.txt").parents
        assert len(p_list) == 1

        p_list = S3Path("bucket").parents
        assert len(p_list) == 0

        with pytest.raises(ValueError):
            _ = S3Path().parents

        with pytest.raises(ValueError):
            _ = (
                S3Path(
                    "bucket",
                    "folder",
                    "subfolder",
                    "file.txt",
                )
                .relative_to(S3Path("bucket"))
                .parents
            )

    def _test_fname(self):
        assert S3Path("bucket", "file").fname == "file"

        with pytest.raises(ValueError):
            _ = S3Path._from_parsed_parts(
                bucket=None,
                parts=[],
                is_dir=False,
            ).fname

    def _test_ext(self):
        assert S3Path("bucket", "file").ext == ""

        with pytest.raises(ValueError):
            _ = S3Path._from_parsed_parts(
                bucket=None,
                parts=[],
                is_dir=False,
            ).ext

    def _test_is_parent_of(self):
        assert S3Path("bkt").is_parent_of(S3Path("bkt/a")) is True
        assert S3Path("bkt").is_parent_of(S3Path("bkt/a/")) is True
        assert S3Path("bkt/a/").is_parent_of(S3Path("bkt/a/b")) is True
        assert S3Path("bkt/a/").is_parent_of(S3Path("bkt/a/b/")) is True

        # root bucket's parent is itself
        assert S3Path("bkt").is_parent_of(S3Path("bkt")) is True

        # for non bucket root directory, parent has to be shorter than other
        assert S3Path("bkt/a/").is_parent_of(S3Path("bkt/a/")) is False
        assert S3Path("bkt/a/b/").is_parent_of(S3Path("bkt/a")) is False

        # different bucket name always returns False
        assert S3Path("bkt1/a/").is_parent_of(S3Path("bkt2/a/b/")) is False

        # has to be concrete S3Path
        with pytest.raises(TypeError):
            S3Path().is_parent_of(S3Path("bkt/a/b/"))

        with pytest.raises(TypeError):
            S3Path("bkt/a/").is_parent_of(S3Path())

        # parent has to be a directory
        with pytest.raises(TypeError):
            S3Path("bkt/a").is_parent_of(S3Path("bkt/a/b/"))

    def _test_is_prefix_of(self):
        assert S3Path("bkt").is_prefix_of(S3Path("bkt/a")) is True
        assert S3Path("bkt").is_prefix_of(S3Path("bkt/a/")) is True
        assert S3Path("bkt/a/").is_prefix_of(S3Path("bkt/a/b")) is True
        assert S3Path("bkt/a/").is_prefix_of(S3Path("bkt/a/b/")) is True
        assert S3Path("bkt").is_prefix_of(S3Path("bkt")) is True
        assert S3Path("bkt/a").is_prefix_of(S3Path("bkt/a")) is True
        assert S3Path("bkt/a/").is_prefix_of(S3Path("bkt/a/")) is True

        assert S3Path("bkt/a/b/").is_prefix_of(S3Path("bkt/a")) is False

        # different bucket name always returns False
        assert S3Path("bkt1/a/").is_prefix_of(S3Path("bkt2/a/b/")) is False

        # has to be concrete S3Path
        with pytest.raises(TypeError):
            S3Path().is_prefix_of(S3Path("bkt/a/b/"))

        with pytest.raises(TypeError):
            S3Path("bkt/a/").is_prefix_of(S3Path())

    def _test_root(self):
        assert S3Path("bkt/a/b/c").root == S3Path("bkt/")
        assert S3Path("bkt/a/b/").root == S3Path("bkt/")
        assert S3Path("bkt").root == S3Path("bkt/")
        with pytest.raises(TypeError):
            _ = S3Path().root

        with pytest.raises(TypeError):
            _ = S3Path.make_relpath("folder").root

    def test(self):
        self._test_properties()
        self._test_parent()
        self._test_parents()
        self._test_fname()
        self._test_ext()
        self._test_is_parent_of()
        self._test_is_prefix_of()
        self._test_root()


class Test(AttributeAPIMixin):
    use_mock = False


class TestUseMock(AttributeAPIMixin):
    use_mock = True


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.core.attribute", preview=False)
