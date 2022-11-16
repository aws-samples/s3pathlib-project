# -*- coding: utf-8 -*-

import pytest
from s3pathlib.core import S3Path
from s3pathlib.tests import run_cov_test


class TestRelativePathAPIMixin:
    def test_relative_path(self):
        # these s3path are equivalent
        p_list = [
            S3Path.make_relpath("a", "b", "c"),
            S3Path.make_relpath("", "a", "b", "c", ""),
            S3Path.make_relpath("a/b/c"),
            S3Path.make_relpath("", "a/b/c", ""),
            S3Path("bucket", "a/b/c").relative_to(S3Path("bucket")),
            S3Path("bucket", "folder/a/b/c").relative_to(S3Path("bucket", "folder/")),
        ]
        for p in p_list:
            assert p._bucket is None
            assert p._parts == ["a", "b", "c"]
            assert p._is_dir is False

        # these s3path are equivalent
        p_list = [
            S3Path.make_relpath(),
            S3Path("bucket", "a/b/c").relative_to(S3Path("bucket", "a/b/c")),
        ]
        for p in p_list:
            assert p._bucket is None
            assert p._parts == []
            assert p._is_dir is None

    def test_mixed_s3path_parts(self):
        p = S3Path(
            S3Path("bucket", "folder"),
            S3Path._from_parsed_parts(
                bucket=None,
                parts=["relpath", "file.txt"],
                is_dir=False,
            ),
        )
        assert p._bucket == "bucket"
        assert p._parts == ["folder", "relpath", "file.txt"]
        assert p._is_dir is False

        # join with relpath
        p = S3Path(
            S3Path("bucket", "folder"),
            S3Path._from_parsed_parts(
                bucket=None,
                parts=[
                    "relpath",
                ],
                is_dir=True,
            ),
        )
        assert p._bucket == "bucket"
        assert p._parts == ["folder", "relpath"]
        assert p._is_dir is True

        p = S3Path(
            S3Path("bucket", "folder"),
            S3Path._from_parsed_parts(
                bucket=None,
                parts=["relpath", "file.txt"],
                is_dir=False,
            ),
        )
        assert p._bucket == "bucket"
        assert p._parts == ["folder", "relpath", "file.txt"]
        assert p._is_dir is False

        # multiple relpath
        p = S3Path(
            S3Path("bucket", "folder"),
            S3Path._from_parsed_parts(
                bucket=None,
                parts=[
                    "relpath1",
                ],
                is_dir=True,
            ),
            S3Path._from_parsed_parts(
                bucket=None,
                parts=[
                    "relpath2",
                ],
                is_dir=True,
            ),
            S3Path._from_parsed_parts(
                bucket=None,
                parts=[
                    "file.txt",
                ],
                is_dir=False,
            ),
        )
        assert p._bucket == "bucket"
        assert p._parts == ["folder", "relpath1", "relpath2", "file.txt"]
        assert p._is_dir is False

    def test_relative_to(self):
        # if self is file, then relative path is also a file
        p = S3Path("bucket", "a", "b", "c").relative_to(S3Path("bucket", "a"))
        assert p.is_relpath()
        assert p._parts == ["b", "c"]
        assert p._is_dir is False

        # if self is dir, then relative path is also a dir
        p = S3Path("bucket", "a", "b", "c/").relative_to(S3Path("bucket", "a"))
        assert p.is_relpath()
        assert p._parts == ["b", "c"]
        assert p._is_dir is True

        # relpath to bucket root
        p = S3Path("bucket", "a", "b").relative_to(S3Path("bucket"))
        assert p.is_relpath()
        assert p._parts == ["a", "b"]
        assert p._is_dir is False

        p = S3Path("bucket", "a", "b/").relative_to(S3Path("bucket"))
        assert p.is_relpath()
        assert p._parts == ["a", "b"]
        assert p._is_dir is True

        # relative to self
        p_list = [
            S3Path("bucket", "a").relative_to(S3Path("bucket", "a")),
            S3Path("bucket", "a/").relative_to(S3Path("bucket", "a/")),
            S3Path("bucket").relative_to(S3Path("bucket")),
        ]
        for p in p_list:
            assert p._bucket is None
            assert p._parts == []
            assert p._is_dir is None

        # exceptions
        with pytest.raises(ValueError):
            S3Path().relative_to(S3Path())

        with pytest.raises(ValueError):
            S3Path("bucket1").relative_to(S3Path("bucket2"))

        with pytest.raises(ValueError):
            S3Path("bucket", "a").relative_to(S3Path("bucket", "b"))

        with pytest.raises(ValueError):
            S3Path("bucket", "a").relative_to(S3Path("bucket", "a", "b"))

    def test_sub_operator(self):
        bucket = S3Path("bucket")
        directory = S3Path("bucket", "folder/")
        file = S3Path("bucket", "folder", "file.txt")
        assert (directory - bucket) == directory.relative_to(bucket)
        assert (file - directory) == file.relative_to(directory)

    def test_exceptions(self):
        p1 = S3Path("bucket1", "a")
        p2 = S3Path("bucket2", "a", "b", "c")
        with pytest.raises(ValueError):
            p2.relative_to(p1)

        p1 = S3Path("bucket", "d")
        p2 = S3Path("bucket", "a", "b", "c")
        with pytest.raises(ValueError):
            p2.relative_to(p1)

    def test_type_test(self):
        """
        Test if the instance is a ...

        - is_dir()
        - is_file()
        - is_relpath()
        - is_bucket()
        - is_void()
        """
        # s3 object
        p = S3Path("bucket", "file.txt")
        assert p.is_relpath() is False
        with pytest.raises(Exception):
            p.ensure_relpath()
        p.ensure_not_relpath()

        # s3 directory
        p = S3Path("bucket", "folder/")
        assert p.is_relpath() is False
        with pytest.raises(Exception):
            p.ensure_relpath()
        p.ensure_not_relpath()

        # s3 bucket
        p = S3Path("bucket")
        assert p.is_relpath() is False
        with pytest.raises(Exception):
            p.ensure_relpath()
        p.ensure_not_relpath()

        # void path
        p = S3Path()
        assert p.is_relpath() is True
        p.ensure_relpath()
        with pytest.raises(Exception):
            p.ensure_not_relpath()

        # relative path
        p = S3Path("bucket", "file.txt").relative_to(S3Path("bucket"))
        assert p.is_void() is False
        assert p.is_dir() is False
        assert p.is_file() is True
        assert p.is_bucket() is False
        assert p.is_relpath() is True
        p.ensure_relpath()
        with pytest.raises(Exception):
            p.ensure_not_relpath()

        p = S3Path("bucket", "folder/").relative_to(S3Path("bucket"))
        assert p.is_void() is False
        assert p.is_dir() is True
        assert p.is_file() is False
        assert p.is_bucket() is False
        assert p.is_relpath() is True
        p.ensure_relpath()
        with pytest.raises(Exception):
            p.ensure_not_relpath()

        p = S3Path("bucket").relative_to(S3Path("bucket"))
        assert p.is_void() is True
        assert p.is_dir() is False
        assert p.is_file() is False
        assert p.is_bucket() is False
        assert p.is_relpath() is True
        p.ensure_relpath()
        with pytest.raises(Exception):
            p.ensure_not_relpath()

        p = S3Path.make_relpath("folder/")
        assert p.is_void() is False
        assert p.is_dir() is True
        assert p.is_file() is False
        assert p.is_bucket() is False
        assert p.is_relpath() is True
        p.ensure_relpath()
        with pytest.raises(Exception):
            p.ensure_not_relpath()

        p = S3Path.make_relpath("file.txt")
        assert p.is_void() is False
        assert p.is_dir() is False
        assert p.is_file() is True
        assert p.is_bucket() is False
        assert p.is_relpath() is True
        p.ensure_relpath()
        with pytest.raises(Exception):
            p.ensure_not_relpath()

        # relpath edge case
        assert (
            S3Path._from_parsed_parts(bucket=None, parts=[], is_dir=True).is_relpath()
            is False
        )

        assert (
            S3Path._from_parsed_parts(bucket=None, parts=[], is_dir=False).is_relpath()
            is False
        )

    def test_uri_properties(self):
        p = S3Path("bucket/folder/file.txt").relative_to(S3Path("bucket"))
        assert p.bucket is None
        assert p.key == "folder/file.txt"
        assert p.parts == ["folder", "file.txt"]
        assert p.uri is None
        assert p.arn is None
        assert p.console_url is None


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.core.relative", open_browser=False)
