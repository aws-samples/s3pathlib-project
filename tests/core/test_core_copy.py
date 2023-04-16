# -*- coding: utf-8 -*-

import pytest
from pathlib_mate import Path
from s3pathlib.core import S3Path
from s3pathlib.tests import bucket, prefix, run_cov_test


dir_here = Path.dir_here(__file__)

s3dir_root = S3Path(bucket, prefix, "core", "copy").to_dir()


class TestCopyAPIMixin:
    def test_copy_object(self):
        # before state
        p_src = S3Path(s3dir_root, "copy-object", "before.py")
        p_src.write_text("a")

        p_dst = S3Path(p_src, "copy-object", "after.py")
        p_dst.delete_if_exists()

        assert p_dst.exists() is False

        # invoke api
        count = p_src.copy_to(p_dst, overwrite=False)

        # after state
        assert count == 1
        assert p_dst.exists() is True

        # raise exception
        with pytest.raises(FileExistsError):
            p_src.copy_to(p_dst, overwrite=False)

    def test_copy_dir(self):
        # before state
        p_src = S3Path(s3dir_root, "copy-dir", "before").to_dir()
        p_src.delete_if_exists()
        assert p_src.count_objects() == 0

        dir_to_upload = dir_here.joinpath("test_upload_dir").abspath
        p_src.upload_dir(
            local_dir=dir_to_upload,
            pattern="**/*.txt",
            overwrite=True,
        )

        p_dst = S3Path(s3dir_root, "copy-dir", "after").to_dir()
        p_dst.delete_if_exists()
        assert p_dst.count_objects() == 0

        # invoke api
        count = p_src.copy_to(dst=p_dst, overwrite=False)

        # validate after state
        assert count == 2

        # raise exception
        with pytest.raises(FileExistsError):
            p_src.copy_to(dst=p_dst, overwrite=False)

    def test_move_to(self):
        # before state
        p_src = S3Path(s3dir_root, "move-to", "before").to_dir()
        p_src.delete_if_exists()
        assert p_src.count_objects() == 0

        dir_to_upload = dir_here.joinpath("test_upload_dir").abspath
        p_src.upload_dir(
            local_dir=dir_to_upload,
            pattern="**/*.txt",
            overwrite=True,
        )

        p_dst = S3Path(s3dir_root, "move-to", "after/")
        p_dst.delete_if_exists()

        assert p_dst.count_objects() == 0

        # invoke api
        count = p_src.move_to(dst=p_dst, overwrite=False)

        # validate after state
        assert count == 2
        assert p_src.count_objects() == 0
        assert p_dst.count_objects() == 2


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.core.copy", preview=False)
