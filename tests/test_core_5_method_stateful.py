# -*- coding: utf-8 -*-

"""
AWS S3 becomes strong read-after-write consistency since 2020-12-01 (see
https://aws.amazon.com/about-aws/whats-new/2020/12/amazon-s3-now-delivers-strong-read-after-write-consistency-automatically-for-all-applications/).

We don't need to put ``time.sleep(1)`` after each write process to wait the change
taking effect.
"""

import os
import pytest
from s3pathlib.aws import context
from s3pathlib.core import S3Path
from s3pathlib.tests import boto_ses, bucket, prefix

context.attach_boto_session(boto_ses)

dir_here = os.path.dirname(os.path.abspath(__file__))
dir_tests = dir_here


class TestS3Path:
    p_root = S3Path(bucket, prefix, "change-state")

    def test_delete_if_exists(self):
        # file
        p = S3Path(self.p_root, "delete-if-exists", "test.py")
        p.upload_file(path=__file__, overwrite=True)

        assert p.delete_if_exists() == 1

        assert p.delete_if_exists() == 0

        # dir
        p = S3Path(self.p_root, "delete-if-exists", "test/")
        p.upload_dir(
            local_dir=os.path.join(dir_tests, "test_upload_dir"),
            overwrite=True,
            pattern="**/*.txt",
        )

        assert p.delete_if_exists() == 2

        assert p.delete_if_exists() == 0

    def test_touch(self):
        p = S3Path(self.p_root, "touch", "test.txt")
        p.delete_if_exists()

        assert p.exists() is False
        p.touch()
        assert p.exists() is True
        assert p.size == 0
        with pytest.raises(FileExistsError):
            p.touch(exist_ok=False)
        p.touch()

        p = S3Path(self.p_root, "touch", "folder/")
        with pytest.raises(TypeError):
            p.touch()

    def test_mkdir(self):
        # case 1, exists_ok = False, parents = True
        p_root = S3Path(self.p_root, "mkdir/")

        # clear off all existing folders
        p_root.delete_if_exists(include_folder=True)

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

        # p_root.delete_if_exists(include_folder=True)

    def test_upload_file(self):
        # before state
        p = S3Path(self.p_root, "upload-file", "test.py")
        p.delete_if_exists()
        assert p.exists() is False

        # invoke api
        p.upload_file(path=__file__, overwrite=True)

        # after state
        assert p.exists() is True

        # raise exception if exists
        with pytest.raises(FileExistsError):
            p.upload_file(path=__file__, overwrite=False)

        # raise type error if upload to a folder
        with pytest.raises(TypeError):
            p = S3Path("bucket", "folder/")
            p.upload_file("/tmp/file.txt")

    def test_upload_dir(self):
        # before state
        p = S3Path(self.p_root, "upload-dir/")
        p.delete_if_exists()
        assert p.count_objects() == 0

        # invoke api
        p.upload_dir(
            local_dir=os.path.join(dir_tests, "test_upload_dir"),
            pattern="**/*.txt",
            overwrite=True,
        )

        # after state
        assert p.count_objects() == 2

        # raise exception if exists
        with pytest.raises(FileExistsError):
            p.upload_dir(
                local_dir=os.path.join(dir_tests, "test_upload_dir"),
                pattern="**/*.txt",
                overwrite=False,
            )

        # raise type error if upload to a folder
        with pytest.raises(TypeError):
            p = S3Path("bucket", "file.txt")
            p.upload_dir("/tmp/folder")

    def test_copy_object(self):
        # before state
        p_src = S3Path(self.p_root, "copy-object", "before.py")
        p_src.upload_file(path=__file__, overwrite=True)

        p_dst = S3Path(self.p_root, "copy-object", "after.py")
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
        p_src = S3Path(self.p_root, "copy-dir", "before/")
        local_dir = os.path.join(dir_tests, "test_upload_dir")
        p_src.upload_dir(
            local_dir=local_dir,
            pattern="**/*.txt",
            overwrite=True,
        )

        p_dst = S3Path(self.p_root, "copy-dir", "after/")
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
        p_src = S3Path(self.p_root, "move-to", "before/")
        local_dir = os.path.join(dir_tests, "test_upload_dir")
        p_src.upload_dir(
            local_dir=local_dir,
            pattern="**/*.txt",
            overwrite=True,
        )

        p_dst = S3Path(self.p_root, "move-to", "after/")
        p_dst.delete_if_exists()

        assert p_dst.count_objects() == 0

        # invoke api
        count = p_src.move_to(dst=p_dst, overwrite=False)

        # validate after state
        assert count == 2
        assert p_src.count_objects() == 0
        assert p_dst.count_objects() == 2


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
