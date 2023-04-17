# -*- coding: utf-8 -*-

import pytest
from s3pathlib.core import S3Path
from s3pathlib.tests import run_cov_test


class TestJoinPathAPIMixin:
    def test_join_path(self):
        p1 = S3Path("bucket", "folder", "subfolder", "file.txt")
        p2 = p1.parent
        p3 = p2.parent
        relpath1 = p1.relative_to(p2)
        relpath2 = p2.relative_to(p3)

        with pytest.warns():
            p4 = p3.join_path(relpath2, relpath1)
            assert p1 == p4

            with pytest.raises(TypeError):
                p3.join_path(p1, p2)

    def test_joinpath(self):
        # ------
        p = S3Path("bucket")
        assert p.joinpath("file.txt").uri == "s3://bucket/file.txt"
        assert p.joinpath("/file.txt").uri == "s3://bucket/file.txt"
        assert p.joinpath("folder/").uri == "s3://bucket/folder/"
        assert p.joinpath("/folder/").uri == "s3://bucket/folder/"
        assert p.joinpath("folder", "file.txt").uri == "s3://bucket/folder/file.txt"

        relpath_folder = S3Path("my-bucket", "data", "folder/").relative_to(
            S3Path("my-bucket", "data")
        )
        assert p.joinpath(relpath_folder).uri == "s3://bucket/folder/"
        assert (
            p.joinpath(
                "data",
                relpath_folder,
                "file.txt",
            ).uri
            == "s3://bucket/data/folder/file.txt"
        )

        # ------
        with pytest.raises(TypeError):
            S3Path("bucket1").joinpath(S3Path("bucket2"))

        # ------
        p = S3Path("bucket", "file.txt")
        p1 = p.joinpath("/")
        assert p1.is_dir()
        assert p1.uri == "s3://bucket/file.txt/"

        p2 = p.joinpath("subfolder", "/")
        assert p2.is_dir()
        assert p2.uri == "s3://bucket/file.txt/subfolder/"

        # ------
        p1 = S3Path("bucket", "folder", "subfolder", "file.txt")
        p2 = p1.parent  # s3://bucket/folder/subfolder
        p3 = p2.parent  # s3://bucket/folder
        with pytest.raises(TypeError):
            p3.joinpath(p1, p2)

        with pytest.raises(TypeError):
            p3.joinpath(1)

    def test_divide_operator(self):
        bucket = S3Path("bucket")
        directory = S3Path("bucket", "folder/")
        file = S3Path("bucket", "folder", "file.txt")
        relpath = S3Path("bucket", "folder", "file.txt").relative_to(S3Path("bucket"))
        void = S3Path()

        assert bucket / "folder/" == directory
        assert bucket / "folder" != directory
        assert (
            bucket
            / [
                "folder/",
            ]
            == directory
        )
        assert bucket / ["folder", "/"] == directory
        assert (
            bucket
            / [
                "folder",
            ]
            != directory
        )

        assert bucket / "folder" / "file.txt" == file
        assert bucket / "folder/" / "file.txt" == file
        assert bucket / "folder/" / "/file.txt" == file

        assert bucket / ["folder", "file.txt"] == file
        assert bucket / ["folder/", "file.txt"] == file
        assert bucket / ["folder/", "/file.txt"] == file

        assert bucket / relpath == file

        p = file / "/"
        assert p.is_dir()
        assert p.uri == "s3://bucket/folder/file.txt/"

        p = file / ["subfolder", "/"]
        assert p.is_dir()
        assert p.uri == "s3://bucket/folder/file.txt/subfolder/"

        root = S3Path("bucket")
        rel1 = S3Path("bucket/folder/").relative_to(S3Path("bucket"))
        rel2 = S3Path("bucket/folder/file.txt").relative_to(S3Path("bucket/folder/"))
        assert root / [rel1, rel2] == S3Path("bucket/folder/file.txt")
        assert root / (rel1 / rel2) == S3Path("bucket/folder/file.txt")

        with pytest.raises(TypeError):  # relpath cannot / non-relpath
            rel1 / root

        with pytest.raises(TypeError):
            void / "bucket"


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.core.joinpath", preview=False)
