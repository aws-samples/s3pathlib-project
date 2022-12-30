# -*- coding: utf-8 -*-

import pytest
from s3pathlib import S3Path, context
from s3pathlib.client import (
    is_bucket_exists,
    head_object,
    head_object_or_none,
    put_object,
    get_object_tagging,
    put_object_tagging,
    update_object_tagging,
    copy_object,
    S3ObjectNotExist,
)

from s3pathlib.tests import boto_ses, s3_client, bucket, prefix

context.attach_boto_session(boto_ses)

NOT_EXIST_STR = "8c8b3db3e28e90a95b4d692f81bcbd71"


class TestClient:
    p_root = S3Path(bucket, prefix, "client")

    def test_is_bucket_exists(self):
        assert is_bucket_exists(s3_client, bucket) is True
        assert is_bucket_exists(s3_client, NOT_EXIST_STR) is False

    def test_head_object(self):
        with pytest.raises(S3ObjectNotExist):
            head_object(s3_client, bucket, NOT_EXIST_STR)

    def test_head_object_or_none(self):
        assert head_object_or_none(s3_client, bucket, NOT_EXIST_STR) is None

    def test_put_object(self):
        p = S3Path(self.p_root, "put_object", "hello.txt")
        p.delete_if_exists()
        p.clear_cache()

        # object not exists
        with pytest.raises(S3ObjectNotExist):
            get_object_tagging(s3_client, p.bucket, p.key)

        # put object without tags
        put_object(
            s3_client,
            p.bucket,
            p.key,
            b"hello",
        )

        # metadata is empty
        assert p.metadata == {}

        # tags is empty
        assert get_object_tagging(s3_client, p.bucket, p.key) == {}

        # use put_object API
        put_object(
            s3_client,
            p.bucket,
            p.key,
            metadata={"file-type": "txt"},
            tags={"k1": "v1"},
        )
        p.clear_cache()
        assert p.size == 0  # content erased, because we just put_object without body
        assert p.metadata == {"file-type": "txt"}
        assert get_object_tagging(s3_client, p.bucket, p.key) == {"k1": "v1"}

        # put object tagging is a full replacement
        put_object_tagging(s3_client, p.bucket, p.key, tags={"k2": "v2", "k3": "v3"})
        assert get_object_tagging(s3_client, p.bucket, p.key) == {"k2": "v2", "k3": "v3"}

        # update object is a "true update" operation
        tags = update_object_tagging(s3_client, p.bucket, p.key, tags={"k1": "v1", "k2": "v22"})
        assert tags == {"k1": "v1", "k2": "v22", "k3": "v3"}

        existing_tags = get_object_tagging(s3_client, p.bucket, p.key)
        assert existing_tags == {"k1": "v1", "k2": "v22", "k3": "v3"}

    def test_copy_object(self):
        p_src = S3Path(self.p_root, "copy_object", "src.txt")
        p_dst = S3Path(self.p_root, "copy_object", "dst.txt")
        p_src.delete_if_exists()
        p_dst.delete_if_exists()

        put_object(
            s3_client,
            p_src.bucket,
            p_src.key,
            b"hello",
            metadata=dict(key_name="a"),
            tags=dict(tag_name="a"),
        )

        # copy without metadata and tags
        copy_object(
            s3_client,
            p_src.bucket,
            p_src.key,
            p_dst.bucket,
            p_dst.key,
        )

        # metadata is empty
        p_dst.clear_cache()
        assert p_dst.metadata == {"key_name": "a"}
        assert p_dst.get_tags() == {"tag_name": "a"}

        # copy with metadata and tags
        copy_object(
            s3_client,
            p_src.bucket,
            p_src.key,
            p_dst.bucket,
            p_dst.key,
            metadata=dict(key_name="b"),
            tags=dict(tag_name="b"),
        )
        p_dst.clear_cache()
        assert p_dst.metadata == {"key_name": "b"}
        assert p_dst.get_tags() == {"tag_name": "b"}


if __name__ == "__main__":
    from s3pathlib.tests import run_cov_test

    run_cov_test(__file__, module="s3pathlib.client", open_browser=False)
