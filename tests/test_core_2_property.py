# -*- coding: utf-8 -*-

import pytest
from s3pathlib.core import S3Path


class TestS3Path:
    def test_properties(self):
        # s3 object
        p = S3Path("bucket", "folder", "file.txt")
        assert p.bucket == "bucket"
        assert p.key == "folder/file.txt"
        assert p.parts == ["folder", "file.txt"]
        assert p.uri == "s3://bucket/folder/file.txt"
        assert p.arn == "arn:aws:s3:::bucket/folder/file.txt"
        assert p.console_url == "https://console.aws.amazon.com/s3/object/bucket?prefix=folder/file.txt"
        assert p.us_gov_cloud_console_url == "https://console.amazonaws-us-gov.com/s3/object/bucket?prefix=folder/file.txt"
        assert str(p) == "S3Path('s3://bucket/folder/file.txt')"
        assert p.basename == "file.txt"
        assert p.fname == "file"
        assert p.ext == ".txt"
        assert p.dirname == "folder"
        assert p.abspath == "/folder/file.txt"
        assert p.dirpath == "/folder/"

        # s3 directory
        p = S3Path("bucket", "folder/")
        assert p.bucket == "bucket"
        assert p.key == "folder/"
        assert p.parts == ["folder", ]
        assert p.uri == "s3://bucket/folder/"
        assert p.arn == "arn:aws:s3:::bucket/folder/"
        assert p.console_url == "https://console.aws.amazon.com/s3/buckets/bucket?prefix=folder/"
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
        assert p.bucket == "bucket"
        assert p.key == ""
        assert p.parts == []
        assert p.uri == "s3://bucket/"
        assert p.arn == "arn:aws:s3:::bucket"
        assert p.console_url == "https://console.aws.amazon.com/s3/buckets/bucket?tab=objects"
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
        assert p.bucket is None
        assert p.key == ""
        assert p.parts == []
        assert p.uri is None
        assert p.arn is None
        assert p.console_url is None
        assert p.us_gov_cloud_console_url is None
        assert str(p) == "S3Path()"
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
        assert p.bucket is None
        assert p.key == "folder/file.txt"
        assert p.parts == ["folder", "file.txt"]
        assert p.uri is None
        assert p.arn is None
        assert p.console_url is None
        assert str(p) == "S3Path('folder/file.txt')"
        assert p.basename == "file.txt"
        assert p.fname == "file"
        assert p.ext == ".txt"
        assert p.dirname == "folder"
        with pytest.raises(TypeError):
            _ = p.abspath
        with pytest.raises(TypeError):
            _ = p.abspath

    def test_parent(self):
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

    def test_parents(self):
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
            _ = S3Path(
                "bucket",
                "folder", "subfolder", "file.txt",
            ).relative_to(S3Path("bucket")).parents

    def test_fname(self):
        assert S3Path("bucket", "file").fname == "file"

        with pytest.raises(ValueError):
            _ = S3Path._from_parsed_parts(
                bucket=None,
                parts=[],
                is_dir=False,
            ).fname

    def test_ext(self):
        assert S3Path("bucket", "file").ext == ""

        with pytest.raises(ValueError):
            _ = S3Path._from_parsed_parts(
                bucket=None,
                parts=[],
                is_dir=False,
            ).ext


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
