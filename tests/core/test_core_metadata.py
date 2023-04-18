# -*- coding: utf-8 -*-

from datetime import datetime

from s3pathlib.core import S3Path
from s3pathlib.utils import md5_binary
from s3pathlib.tests import run_cov_test
from s3pathlib.tests.mock import BaseTest


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

    def _test_attributes(self):
        p = self.p

        assert p.etag == md5_binary("Hello World!".encode("utf-8"))
        _ = p.last_modified_at
        assert p.size == 12
        assert p.size_for_human == "12 B"
        assert p.version_id is None
        assert p.expire_at is None
        assert p.metadata == {"creator": "Alice"}

    def _test_clear_cache(self):
        p = self.p

        p.clear_cache()
        assert p._meta is None
        assert len(p.etag) == 32
        assert isinstance(p._meta, dict)

    def _test_from_content_dict(self):
        s3path = S3Path._from_content_dict(
            bucket="bucket",
            dct={
                "Key": "file.txt",
                "LastModified": datetime(2015, 1, 1),
                "ETag": "'string'",
                "ChecksumAlgorithm": "SHA256",
                "Size": 123,
                "StorageClass": "STANDARD",
                "Owner": {"DisplayName": "string", "ID": "string"},
            },
        )
        assert s3path.key == "file.txt"
        assert s3path.last_modified_at == datetime(2015, 1, 1)
        assert s3path.etag == "string"
        assert s3path.size == 123

    def _test_object_metadata(self):
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

    def test(self):
        self._test_attributes()
        self._test_clear_cache()
        self._test_from_content_dict()
        self._test_object_metadata()


class Test(MetadataAPIMixin):
    use_mock = False


class TestUseMock(MetadataAPIMixin):
    use_mock = True


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.core.metadata", preview=False)
