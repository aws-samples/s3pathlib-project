# -*- coding: utf-8 -*-

import pytest
from s3pathlib.aws import context
from s3pathlib.core import S3Path
from s3pathlib.tests import boto_ses, bucket, prefix
from s3pathlib.utils import md5_binary

context.attach_boto_session(boto_ses)
s3_client = context.s3_client


class TestS3Path:
    _root = S3Path(bucket, prefix, "stateless/")
    p = S3Path(_root, "file.txt")
    p_empty_object = S3Path(_root, "empty.txt")
    p_soft_folder_file = S3Path(_root, "soft_folder", "file.txt")
    p_hard_folder = S3Path(_root, "hard_folder/")
    p_hard_folder_file = S3Path(_root, "hard_folder", "file.txt")
    p_empty_folder = S3Path(_root, "empty_folder/")
    p_statistics = S3Path(_root, "statistics/")

    @classmethod
    def setup_class(cls):
        cls.setup_for_exists()
        cls.setup_for_statistics()

    @classmethod
    def setup_for_exists(cls):
        s3_client.put_object(
            Bucket=cls.p.bucket,
            Key=cls.p.key,
            Body="Hello World!",
            Metadata={
                "creator": "Alice"
            }
        )
        s3_client.put_object(
            Bucket=cls.p_empty_object.bucket,
            Key=cls.p_empty_object.key,
            Body="",
            Metadata={
                "creator": "Alice"
            }
        )
        s3_client.put_object(
            Bucket=cls.p_soft_folder_file.bucket,
            Key=cls.p_soft_folder_file.key,
            Body="Hello World!",
        )
        s3_client.put_object(
            Bucket=cls.p_hard_folder.bucket,
            Key=cls.p_hard_folder.key,
            Body="",
        )
        s3_client.put_object(
            Bucket=cls.p_hard_folder_file.bucket,
            Key=cls.p_hard_folder_file.key,
            Body="Hello World!",
        )
        s3_client.put_object(
            Bucket=cls.p_empty_folder.bucket,
            Key=cls.p_empty_folder.key,
            Body="",
        )

    @classmethod
    def setup_for_statistics(cls):
        for i in range(1, 1 + 4):
            s3_client.put_object(
                Bucket=cls.p_statistics.bucket,
                Key=S3Path(cls.p_statistics, "folder1", f"{i}.txt").key,
                Body="Hello World!",
            )

        for i in range(1, 1 + 6):
            s3_client.put_object(
                Bucket=cls.p_statistics.bucket,
                Key=S3Path(cls.p_statistics, "folder2", f"{i}.json").key,
                Body="{\"message\": \"Hello World!\"}",
            )

    def test_attributes(self):
        p = self.p
        assert p.etag == md5_binary("Hello World!".encode("utf-8"))
        _ = p.last_modified_at
        assert p.size == 12
        assert p.size_for_human == "12 B"
        assert p.version_id is None
        assert p.expire_at is None
        assert p.metadata == {"creator": "Alice"}

    def test_clear_cache(self):
        p = self.p
        p.clear_cache()
        assert p._meta is None
        assert len(p.etag) == 32
        assert isinstance(p._meta, dict)

    def test_exists(self):
        # s3 bucket
        assert S3Path(bucket).exists() is True
        # have access but not exists
        assert S3Path(f"{bucket}-not-exists").exists() is False
        # doesn't have access
        with pytest.raises(Exception):
            assert S3Path("asdf").exists() is False

        # s3 object
        p = self.p
        assert p.exists() is True

        p = S3Path(bucket, "this-never-gonna-exists.exe")
        assert p.exists() is False

        # s3 directory
        p = self.p.parent
        assert p.exists() is True

        p = S3Path(bucket, "this-never-gonna-exists/")
        assert p.exists() is False

        # void path
        p = S3Path()
        with pytest.raises(TypeError):
            p.exists()

        # relative path
        p = S3Path.make_relpath("folder", "file.txt")
        with pytest.raises(TypeError):
            p.exists()

        # soft / hard / empty folder
        assert self.p_soft_folder_file.parent.exists() is True
        assert self.p_soft_folder_file.exists() is True
        assert self.p_hard_folder.exists() is True
        assert self.p_hard_folder_file.exists() is True
        assert self.p_empty_folder.exists() is True

    def test_statistics(self):
        p = self.p_statistics

        assert p.count_objects() == 10

        count, total_size = p.calculate_total_size()
        assert count == 10
        assert total_size == 210

        count, total_size = p.calculate_total_size(for_human=True)
        assert count == 10
        assert total_size == "210 B"

        # soft / hard / empty folder
        assert self.p_soft_folder_file.parent.count_objects() == 1
        assert self.p_hard_folder.count_objects() == 1
        assert self.p_empty_folder.count_objects() == 0


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
