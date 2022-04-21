# -*- coding: utf-8 -*-

"""
Test the S3Path instance constructor functions.
"""

import pytest
from s3pathlib.core import S3Path


class TestS3Path:
    """
    Test strategy, try different construction method, inspect internal
    implementation variables.

    **中文文档**

    测试策略, 根据主要的内部属性的值不同, 可以将 S3Path 分为 5 类. 该测试中对于每一类都创建了
    许多逻辑上是相同的 S3Path, 也就是说这些 S3Path 的构建方式虽然不同, 但是内部属性完全一致,
    可以理解为同一个. 在之后的 API 测试中, 每一类我们只需要取一个用例进行测试即可, 因为其他的
    变种在内部实现上都是一致的.
    """

    def test_classic_aws_s3_object(self):
        # these s3path are equivalent
        p_list = [
            S3Path("bucket", "a", "b", "c"),
            S3Path("bucket/a/b/c"),
            S3Path("bucket", "a/b/c"),
            S3Path("bucket", "/a/b/c"),
            S3Path(S3Path("//bucket//"), "/a/b/c"),
        ]
        for p in p_list:
            assert p._bucket == "bucket"
            assert p._parts == ["a", "b", "c"]
            assert p._is_dir is False

    def test_logical_aws_s3_directory(self):
        # these s3path are equivalent
        p_list = [
            S3Path("bucket", "a", "b", "c/"),
            S3Path("bucket", "/a", "b", "c/"),
            S3Path("//bucket", "a//b//c//"),
            S3Path("//bucket", "//a//b//c//"),
            S3Path("bucket//a//b//c//"),
            S3Path("//bucket//a//b//c//"),
            S3Path(S3Path("//bucket//"), "//a//b//c//"),
        ]
        for p in p_list:
            assert p._bucket == "bucket"
            assert p._parts == ["a", "b", "c"]
            assert p._is_dir is True

    def test_aws_s3_bucket(self):
        # these s3path are equivalent
        p_list = [
            S3Path("bucket"),
            S3Path("/bucket"),
            S3Path("//bucket//"),
            S3Path(S3Path("//bucket//"))
        ]
        for p in p_list:
            assert p._bucket == "bucket"
            assert p._parts == []
            assert p._is_dir is True

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
            S3Path("bucket", "a/b/c").relative_to(S3Path("bucket", "a/b/c"))
        ]
        for p in p_list:
            assert p._bucket is None
            assert p._parts == []
            assert p._is_dir is None

    def test_void_aws_s3_path(self):
        # these s3path are equivalent
        p_list = [
            S3Path(),
            S3Path(S3Path(), S3Path(), S3Path())
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
            )
        )
        assert p._bucket == "bucket"
        assert p._parts == ["folder", "relpath", "file.txt"]
        assert p._is_dir is False

        # join with relpath
        p = S3Path(
            S3Path("bucket", "folder"),
            S3Path._from_parsed_parts(
                bucket=None,
                parts=["relpath", ],
                is_dir=True,
            )
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
            )
        )
        assert p._bucket == "bucket"
        assert p._parts == ["folder", "relpath", "file.txt"]
        assert p._is_dir is False

        # multiple relpath
        p = S3Path(
            S3Path("bucket", "folder"),
            S3Path._from_parsed_parts(
                bucket=None,
                parts=["relpath1", ],
                is_dir=True,
            ),
            S3Path._from_parsed_parts(
                bucket=None,
                parts=["relpath2", ],
                is_dir=True,
            ),
            S3Path._from_parsed_parts(
                bucket=None,
                parts=["file.txt", ],
                is_dir=False,
            ),
        )
        assert p._bucket == "bucket"
        assert p._parts == ["folder", "relpath1", "relpath2", "file.txt"]
        assert p._is_dir is False

    def test_from_s3_uri(self):
        p = S3Path.from_s3_uri("s3://bucket/")
        assert p._bucket == "bucket"
        assert p._parts == []
        assert p._is_dir is True

        p = S3Path.from_s3_uri("s3://bucket/folder/")
        assert p._bucket == "bucket"
        assert p._parts == ["folder", ]
        assert p._is_dir is True

        p = S3Path.from_s3_uri("s3://bucket/folder/file.txt")
        assert p._bucket == "bucket"
        assert p._parts == ["folder", "file.txt"]
        assert p._is_dir is False

    def test_from_s3_arn(self):
        p = S3Path.from_s3_arn("arn:aws:s3:::bucket")
        assert p._bucket == "bucket"
        assert p._parts == []
        assert p._is_dir is True

        p = S3Path.from_s3_arn("arn:aws:s3:::bucket/")
        assert p._bucket == "bucket"
        assert p._parts == []
        assert p._is_dir is True

        p = S3Path.from_s3_arn("arn:aws:s3:::bucket/folder/")
        assert p._bucket == "bucket"
        assert p._parts == ["folder", ]
        assert p._is_dir is True

        p = S3Path.from_s3_arn("arn:aws:s3:::bucket/folder/file.txt")
        assert p._bucket == "bucket"
        assert p._parts == ["folder", "file.txt"]
        assert p._is_dir is False

    def test_exceptions(self):
        with pytest.raises(TypeError):
            S3Path(1)

        with pytest.raises(TypeError):
            S3Path("bucket", 1)


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
