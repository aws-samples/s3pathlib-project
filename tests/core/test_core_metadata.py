# -*- coding: utf-8 -*-

from datetime import datetime
from s3pathlib.core import S3Path
from s3pathlib.utils import md5_binary
from s3pathlib.tests import s3_client, bucket, prefix, run_cov_test


s3dir_root = S3Path(bucket, prefix, "core", "metadata/")


class TestMetadataAPIMixin:
    p = S3Path(s3dir_root, "file.txt")
    p_empty_object = S3Path(s3dir_root, "empty.txt")
    p_soft_folder_file = S3Path(s3dir_root, "soft_folder", "file.txt")
    p_hard_folder = S3Path(s3dir_root, "hard_folder/")
    p_hard_folder_file = S3Path(s3dir_root, "hard_folder", "file.txt")
    p_empty_folder = S3Path(s3dir_root, "empty_folder/")
    p_statistics = S3Path(s3dir_root, "statistics/")

    @classmethod
    def setup_for_exists(cls):
        s3_client.put_object(
            Bucket=cls.p.bucket,
            Key=cls.p.key,
            Body="Hello World!",
            Metadata={"creator": "Alice"},
        )
        s3_client.put_object(
            Bucket=cls.p_empty_object.bucket,
            Key=cls.p_empty_object.key,
            Body="",
            Metadata={"creator": "Alice"},
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
                Body='{"message": "Hello World!"}',
            )

    @classmethod
    def setup_class(cls):
        cls.setup_for_exists()
        cls.setup_for_statistics()

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

    def test_from_content_dict(self):
        s3path = S3Path._from_content_dict(
            bucket="bucket",
            dct={
                "Key": "file.txt",
                "LastModified": datetime(2015, 1, 1),
                "ETag": "string",
                "ChecksumAlgorithm": "SHA256",
                "Size": 123,
                "StorageClass": "STANDARD",
                "Owner": {"DisplayName": "string", "ID": "string"},
            },
        )

    def test_object_metadata(self):
        s3path = s3dir_root / "object-metadata.txt"
        s3_client.put_object(
            Bucket=s3path.bucket,
            Key=s3path.key,
            Body="Hello World!",
        )
        assert s3path.metadata == {} # if no user metadata, then it will return {}

        s3_client.put_object(
            Bucket=s3path.bucket,
            Key=s3path.key,
            Body="Hello World!",
            Metadata={"key": "value"},
        )
        s3path._meta = {"ETag": "abcd"}
        assert s3path.metadata == {"key": "value"}


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.core.metadata", open_browser=False)
