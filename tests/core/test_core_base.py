# -*- coding: utf-8 -*-

import pytest
from s3pathlib.core import S3Path
from s3pathlib.tests import run_cov_test


class TestBaseS3Path:
    """
    Test strategy, try different construction method, inspect internal
    implementation variables.

    **中文文档**

    测试策略, 根据主要的内部属性的值不同, 可以将 S3Path 分为 4 类. 该测试中对于每一类都创建了
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
            assert p.parts == ["a", "b", "c"]

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
            assert p.parts == ["a", "b", "c"]

    def test_aws_s3_bucket(self):
        # these s3path are equivalent
        p_list = [
            S3Path("bucket"),
            S3Path("/bucket"),
            S3Path("//bucket//"),
            S3Path(S3Path("//bucket//")),
        ]
        for p in p_list:
            assert p._bucket == "bucket"
            assert p._parts == []
            assert p._is_dir is True
            assert p.parts == []

    def test_void_aws_s3_path(self):
        # these s3path are equivalent
        p_list = [S3Path(), S3Path(S3Path(), S3Path(), S3Path())]
        for p in p_list:
            assert p._bucket is None
            assert p._parts == []
            assert p._is_dir is None
            assert p.parts == []

    def test_type_error(self):
        with pytest.raises(TypeError):
            S3Path(1, "a", "b", "c")

        with pytest.raises(TypeError):
            S3Path("bucket", 1, 2, 3)

        with pytest.raises(TypeError):
            S3Path(S3Path("bucket"), S3Path("a", "b", "c"))


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.core.base", open_browser=False)
