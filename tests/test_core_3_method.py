# -*- coding: utf-8 -*-

"""
Test those stateless, non API call method
"""

import pytest
from s3pathlib.core import S3Path


class TestS3Path:
    def test_serialization(self):
        p1 = S3Path("bucket", "folder", "file.txt")
        assert p1.to_dict() == {
            "bucket": "bucket",
            "parts": ["folder", "file.txt"],
            "is_dir": False,
        }
        p2 = S3Path.from_dict(p1.to_dict())
        assert p1 == p2
        assert p1 is not p2

    def test_comparison_and_hash(self):
        """
        Test comparison operator

        - ``==``
        - ``!=``
        - ``>``
        - ``<``
        - ``>=``
        - ``<=``
        - ``hash(S3Path())``
        """
        p1 = S3Path("bucket", "file.txt")
        p2 = S3Path("bucket", "folder/")
        p3 = S3Path("bucket")
        p4 = S3Path()
        p5 = S3Path("bucket", "file.txt").relative_to(S3Path("bucket"))
        p6 = S3Path("bucket", "folder/").relative_to(S3Path("bucket"))
        p7 = S3Path("bucket").relative_to(S3Path("bucket"))
        p8 = S3Path.make_relpath("file.txt")
        p9 = S3Path.make_relpath("folder/")

        p_list = [
            p1, p2, p3, p4, p5, p6, p7, p8, p9
        ]
        for p in p_list:
            assert p == p

        assert p1 != p2
        assert p5 != p6
        assert p4 == p7
        assert p5 == p8
        assert p6 == p9

        assert p1 > p3
        assert p1 >= p3

        assert p3 < p1
        assert p3 <= p1

        assert p2 > p3
        assert p2 >= p3

        assert p3 < p2
        assert p3 <= p2

        p_set = set(p_list)
        assert len(p_set) == 6

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
        assert p.is_void() is False
        assert p.is_dir() is False
        assert p.is_file() is True
        assert p.is_bucket() is False
        assert p.is_relpath() is False

        # s3 directory
        p = S3Path("bucket", "folder/")
        assert p.is_void() is False
        assert p.is_dir() is True
        assert p.is_file() is False
        assert p.is_bucket() is False
        assert p.is_relpath() is False

        # s3 bucket
        p = S3Path("bucket")
        assert p.is_void() is False
        assert p.is_dir() is True
        assert p.is_file() is False
        assert p.is_bucket() is True
        assert p.is_relpath() is False

        # void path
        p = S3Path()
        assert p.is_void() is True
        assert p.is_dir() is False
        assert p.is_file() is False
        assert p.is_bucket() is False
        assert p.is_relpath() is True

        # relative path
        p = S3Path("bucket", "file.txt").relative_to(S3Path("bucket"))
        assert p.is_void() is False
        assert p.is_dir() is False
        assert p.is_file() is True
        assert p.is_bucket() is False
        assert p.is_relpath() is True

        p = S3Path("bucket", "folder/").relative_to(S3Path("bucket"))
        assert p.is_void() is False
        assert p.is_dir() is True
        assert p.is_file() is False
        assert p.is_bucket() is False
        assert p.is_relpath() is True

        p = S3Path("bucket").relative_to(S3Path("bucket"))
        assert p.is_void() is True
        assert p.is_dir() is False
        assert p.is_file() is False
        assert p.is_bucket() is False
        assert p.is_relpath() is True

        p = S3Path.make_relpath("folder/")
        assert p.is_void() is False
        assert p.is_dir() is True
        assert p.is_file() is False
        assert p.is_bucket() is False
        assert p.is_relpath() is True

        p = S3Path.make_relpath("file.txt")
        assert p.is_void() is False
        assert p.is_dir() is False
        assert p.is_file() is True
        assert p.is_bucket() is False
        assert p.is_relpath() is True

        # relpath edge case
        assert S3Path._from_parsed_parts(
            bucket=None, parts=[], is_dir=True
        ).is_relpath() is False

        assert S3Path._from_parsed_parts(
            bucket=None, parts=[], is_dir=False
        ).is_relpath() is False

    def test_is_parent_of(self):
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

    def test_is_prefix_of(self):
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

    def test_join_path(self):
        p1 = S3Path("bucket", "folder", "subfolder", "file.txt")
        p2 = p1.parent
        p3 = p2.parent
        relpath1 = p1.relative_to(p2)
        relpath2 = p2.relative_to(p3)
        p4 = p3.join_path(relpath2, relpath1)
        assert p4.to_dict() == p1.to_dict()

        with pytest.raises(TypeError):
            p3.join_path(p1, p2)

    def test_copy(self):
        p1 = S3Path()
        p2 = p1.copy()
        assert p1 is not p2

    def test_change(self):
        p = S3Path("bkt", "a", "b", "c.jpg")

        p1 = p.change()
        assert p1 == p
        assert p1 is not p

        p1 = p.change(new_bucket="bkt1")
        assert p1.uri == "s3://bkt1/a/b/c.jpg"

        p1 = p.change(new_abspath="x/y/z.png")
        assert p1.uri == "s3://bkt/x/y/z.png"

        p1 = p.change(new_ext=".png")
        assert p1.uri == "s3://bkt/a/b/c.png"

        p1 = p.change(new_fname="d")
        assert p1.uri == "s3://bkt/a/b/d.jpg"

        p1 = p.change(new_basename="d.png")
        assert p1.uri == "s3://bkt/a/b/d.png"

        p1 = p.change(new_basename="d/")
        assert p1.uri == "s3://bkt/a/b/d/"
        assert p1.is_dir()

        p1 = p.change(new_dirname="d/")
        assert p1.uri == "s3://bkt/a/d/c.jpg"

        p1 = p.change(new_dirpath="x/y/")
        assert p1.uri == "s3://bkt/x/y/c.jpg"

        p1 = S3Path.make_relpath("a/b/c.jpg")
        p2 = p1.change(new_basename="d.png")
        assert p2._bucket is None
        assert p2._parts == ["a", "b", "d.png"]
        assert p2._is_dir is False

        p2 = p1.change(new_basename="d/")
        assert p2._bucket is None
        assert p2._parts == ["a", "b", "d"]
        assert p2._is_dir is True

        with pytest.raises(ValueError):
            p1 = S3Path()
            p1.change(new_dirpath="x", new_dirname="y")

        with pytest.raises(ValueError):
            p.change(new_abspath="x/y/z.png", new_basename="file.txt")

        with pytest.raises(ValueError):
            p.change(new_dirpath="x", new_dirname="y")

        with pytest.raises(ValueError):
            p.change(new_basename="x", new_fname="y", new_ext=".zip")

    def test_to_dir(self):
        p1 = S3Path("bkt", "a/")
        p2 = p1.to_dir()
        assert p2.is_dir()
        assert p2 == p1
        assert p2 is not p1

        p3 = S3Path("bkt", "a")
        p4 = p3.to_dir()
        assert p4.is_dir()
        assert p4 == p1

        with pytest.raises(ValueError):
            _ = S3Path().to_dir()

    def test_to_file(self):
        p1 = S3Path("bkt", "a")
        p2 = p1.to_file()
        assert p2.is_file()
        assert p2 == p1
        assert p2 is not p1

        p3 = S3Path("bkt", "a/")
        p4 = p3.to_file()
        assert p4.is_file()
        assert p4 == p1

        with pytest.raises(ValueError):
            _ = S3Path().to_file()

    def test_ensure_object(self):
        with pytest.raises(TypeError):
            S3Path("bucket", "folder/").ensure_object()
        S3Path("bucket", "file.txt").ensure_object()

    def test_ensure_dir(self):
        with pytest.raises(TypeError):
            S3Path("bucket", "file.txt").ensure_dir()
        S3Path("bucket", "folder/").ensure_dir()

    def test_ensure_not_relpath(self):
        # not relpath
        S3Path("bucket", "file.txt").ensure_not_relpath()

        # not relpath
        S3Path("bucket", "folder/").ensure_not_relpath()

        # is relpath
        with pytest.raises(TypeError):
            p = S3Path._from_parsed_parts(
                bucket=None, parts=["a", "b"], is_dir=True
            ).ensure_not_relpath()

        # is relpath
        with pytest.raises(TypeError):
            S3Path._from_parsed_parts(
                bucket=None, parts=["a", "b"], is_dir=False
            ).ensure_not_relpath()

        # is relpath
        with pytest.raises(TypeError):
            S3Path._from_parsed_parts(
                bucket=None, parts=["a", "b"], is_dir=None
            ).ensure_not_relpath()


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
