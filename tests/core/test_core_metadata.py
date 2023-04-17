# -*- coding: utf-8 -*-

from datetime import datetime

import pytest

from s3pathlib.core import S3Path
from s3pathlib.core.metadata import alert_upper_case
from s3pathlib.utils import md5_binary
from s3pathlib.tests import run_cov_test
from s3pathlib.tests.mock import BaseTest


def test_alert_upper_case():
    with pytest.warns(UserWarning):
        alert_upper_case(metadata={"Hello": "World"})


class MetadataAPIMixin(BaseTest):
    module = "core.metadata"

    p: S3Path

    @classmethod
    def custom_setup_class(cls):
        cls.p = S3Path(cls.get_s3dir_root(), "file.txt")
        cls.bsm.s3_client.put_object(
            Bucket=cls.p.bucket,
            Key=cls.p.key,
            Body="Hello World!",
            Metadata={"creator": "Alice"},
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
        s3path = S3Path(self.s3dir_root, "object-metadata.txt")
        self.s3_client.put_object(
            Bucket=s3path.bucket,
            Key=s3path.key,
            Body="Hello World!",
        )
        assert s3path.metadata == {}  # if no user metadata, then it will return {}

        self.s3_client.put_object(
            Bucket=s3path.bucket,
            Key=s3path.key,
            Body="Hello World!",
            Metadata={"key": "value"},
        )
        s3path._meta = {"ETag": "abcd"}
        assert s3path.metadata == {"key": "value"}


class Test(MetadataAPIMixin):
    use_mock = False


class TestUseMock(MetadataAPIMixin):
    use_mock = True


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.core.metadata", preview=False)
