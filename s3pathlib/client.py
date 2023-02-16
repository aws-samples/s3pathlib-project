# -*- coding: utf-8 -*-

"""
S3 botocore client wrapper.

Note:

- This module is not for public API
"""

import typing as T
from datetime import datetime

import botocore.exceptions
from func_args import NOTHING, resolve_kwargs

from .type import TagType, MetadataType
from .exc import S3ObjectNotExist
from .tag import parse_tags, encode_tag_set, encode_url_query


def is_bucket_exists(
    s3_client,
    bucket: str,
) -> bool:
    """
    Use head_bucket() api to check if an S3 bucket exists.

    wrapper of `head_bucket <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.head_bucket>`_
    """
    try:
        s3_client.head_bucket(Bucket=bucket)
        return True
    except botocore.exceptions.ClientError as e:
        if "Not Found" in str(e):
            return False
        else:  # pragma: no cover
            raise e


def head_object(
    s3_client,
    bucket: str,
    key: str,
    if_match: str = NOTHING,
    if_modified_since: datetime = NOTHING,
    if_none_match: str = NOTHING,
    if_unmodified_since: datetime = NOTHING,
    range: str = NOTHING,
    version_id: str = NOTHING,
    sse_customer_algorithm: str = NOTHING,
    sse_customer_key: str = NOTHING,
    request_payer: str = NOTHING,
    part_number: int = NOTHING,
    expected_bucket_owner: str = NOTHING,
    checksum_mode: str = NOTHING,
) -> dict:
    """
    Use head_object() api to return metadata of an object.

    wrapper of `head_object <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.head_object>`_
    """
    try:
        dct = s3_client.head_object(
            **resolve_kwargs(
                Bucket=bucket,
                Key=key,
                IfMatch=if_match,
                IfModifiedSince=if_modified_since,
                IfNoneMatch=if_none_match,
                IfUnmodifiedSince=if_unmodified_since,
                Range=range,
                VersionId=version_id,
                SSECustomerAlgorithm=sse_customer_algorithm,
                SSECustomerKey=sse_customer_key,
                RequestPayer=request_payer,
                PartNumber=part_number,
                ExpectedBucketOwner=expected_bucket_owner,
                ChecksumMode=checksum_mode,
            )
        )
        del dct["ResponseMetadata"]
        return dct
    except botocore.exceptions.ClientError as e:
        if "Not Found" in str(e):
            raise S3ObjectNotExist(str(e))
        else:  # pragma: no cover
            raise e


def head_object_or_none(
    s3_client,
    bucket: str,
    key: str,
) -> T.Optional[dict]:
    """
    Use head_object() api to return metadata of an object.

    Behavior:

    1. return ``dict`` head_object() api response if the object exists.
    2. return ``None`` if object does not exist.
    3. raise exception if other error raised.
    """
    try:
        return head_object(s3_client, bucket, key)
    except S3ObjectNotExist:
        return None


def put_object(
    s3_client,
    bucket: str,
    key: str,
    body: bytes = NOTHING,
    metadata: MetadataType = NOTHING,
    tags: TagType = NOTHING,
    acl: str = NOTHING,
    cache_control: str = NOTHING,
    content_disposition: str = NOTHING,
    content_encoding: str = NOTHING,
    content_language: str = NOTHING,
    content_length: int = NOTHING,
    content_md5: str = NOTHING,
    content_type: str = NOTHING,
    checksum_algorithm: str = NOTHING,
    checksum_crc32: str = NOTHING,
    checksum_crc32c: str = NOTHING,
    checksum_sha1: str = NOTHING,
    checksum_sha256: str = NOTHING,
    expires_datetime: datetime = NOTHING,
    grant_full_control: str = NOTHING,
    grant_read: str = NOTHING,
    grant_read_acp: str = NOTHING,
    grant_write_acp: str = NOTHING,
    server_side_encryption: str = NOTHING,
    storage_class: str = NOTHING,
    website_redirect_location: str = NOTHING,
    sse_customer_algorithm: str = NOTHING,
    sse_customer_key: str = NOTHING,
    sse_kms_key_id: str = NOTHING,
    sse_kms_encryption_context: str = NOTHING,
    bucket_key_enabled: bool = NOTHING,
    request_payer: str = NOTHING,
    object_lock_mode: str = NOTHING,
    object_lock_retain_until_datetime: datetime = NOTHING,
    object_lock_legal_hold_status: str = NOTHING,
    expected_bucket_owner: str = NOTHING,
) -> dict:
    """
    wrapper of `put_object <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.put_object>`_

    .. versionchanged:: 1.4.1

        add all of boto3 argument.
    """
    return s3_client.put_object(
        **resolve_kwargs(
            Bucket=bucket,
            Key=key,
            Body=body,
            Metadata=metadata,
            Tagging=tags if tags is NOTHING else encode_url_query(tags),
            ACL=acl,
            CacheControl=cache_control,
            ContentDisposition=content_disposition,
            ContentEncoding=content_encoding,
            ContentLanguage=content_language,
            ContentLength=content_length,
            ContentMD5=content_md5,
            ContentType=content_type,
            ChecksumAlgorithm=checksum_algorithm,
            ChecksumCRC32=checksum_crc32,
            ChecksumCRC32C=checksum_crc32c,
            ChecksumSHA1=checksum_sha1,
            ChecksumSHA256=checksum_sha256,
            Expires=expires_datetime,
            GrantFullControl=grant_full_control,
            GrantRead=grant_read,
            GrantReadACP=grant_read_acp,
            GrantWriteACP=grant_write_acp,
            ServerSideEncryption=server_side_encryption,
            StorageClass=storage_class,
            WebsiteRedirectLocation=website_redirect_location,
            SSECustomerAlgorithm=sse_customer_algorithm,
            SSECustomerKey=sse_customer_key,
            SSEKMSKeyId=sse_kms_key_id,
            SSEKMSEncryptionContext=sse_kms_encryption_context,
            BucketKeyEnabled=bucket_key_enabled,
            RequestPayer=request_payer,
            ObjectLockMode=object_lock_mode,
            ObjectLockRetainUntilDate=object_lock_retain_until_datetime,
            ObjectLockLegalHoldStatus=object_lock_legal_hold_status,
            ExpectedBucketOwner=expected_bucket_owner,
        )
    )


def get_object_tagging(
    s3_client,
    bucket: str,
    key: str,
) -> TagType:
    """
    wrapper of `get_object_tagging <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_object_tagging>`_
    """
    kwargs = dict(
        Bucket=bucket,
        Key=key,
    )
    try:
        response = s3_client.get_object_tagging(**kwargs)
        return parse_tags(response["TagSet"])
    except Exception as e:
        if "The specified key does not exist" in str(e):
            raise S3ObjectNotExist(f"s3://{bucket}/{key}")
        else:  # pragma: no cover
            raise e


def put_object_tagging(
    s3_client,
    bucket: str,
    key: str,
    tags: TagType,
):
    """
    wrapper of `put_object_tagging <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_object_tagging>`_
    """
    kwargs = dict(
        Bucket=bucket,
        Key=key,
        Tagging={"TagSet": encode_tag_set(tags)},
    )
    return s3_client.put_object_tagging(**kwargs)


def update_object_tagging(
    s3_client,
    bucket: str,
    key: str,
    tags: TagType,
) -> TagType:
    """
    Allow you to use ``dict.update`` liked API to update s3 object tagging.
    """
    existing_tags = get_object_tagging(s3_client, bucket, key)
    existing_tags.update(tags)
    put_object_tagging(s3_client, bucket, key, existing_tags)
    return existing_tags


def copy_object(
    s3_client,
    src_bucket: str,
    src_key: str,
    dst_bucket: str,
    dst_key: str,
    metadata: T.Optional[MetadataType] = NOTHING,
    tags: T.Optional[TagType] = NOTHING,
    acl: str = NOTHING,
    cache_control: str = NOTHING,
    content_disposition: str = NOTHING,
    content_encoding: str = NOTHING,
    content_language: str = NOTHING,
    content_md5: str = NOTHING,
    content_type: str = NOTHING,
    copy_source_if_match: str = NOTHING,
    copy_source_if_modified_since: datetime = NOTHING,
    copy_source_if_none_match: str = NOTHING,
    copy_source_if_unmodified_since: datetime = NOTHING,
    expires_datetime: datetime = NOTHING,
    grant_full_control: str = NOTHING,
    grant_read: str = NOTHING,
    grant_read_acp: str = NOTHING,
    grant_write_acp: str = NOTHING,
    server_side_encryption: str = NOTHING,
    storage_class: str = NOTHING,
    website_redirect_location: str = NOTHING,
    sse_customer_algorithm: str = NOTHING,
    sse_customer_key: str = NOTHING,
    sse_kms_key_id: str = NOTHING,
    sse_kms_encryption_context: str = NOTHING,
    bucket_key_enabled: bool = NOTHING,
    copy_source_sse_customer_algorithm: str = NOTHING,
    copy_source_sse_customer_key: str = NOTHING,
    request_payer: str = NOTHING,
    object_lock_mode: str = NOTHING,
    object_lock_retain_until_datetime: datetime = NOTHING,
    object_lock_legal_hold_status: str = NOTHING,
    expected_bucket_owner: str = NOTHING,
    expected_source_bucket_owner: str = NOTHING,
) -> dict:
    """
    wrapper of `copy_object <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.copy_object>`_
    """
    kwargs = dict(
        Bucket=dst_bucket,
        Key=dst_key,
        CopySource=dict(
            Bucket=src_bucket,
            Key=src_key,
        ),
    )
    if metadata is not NOTHING:
        kwargs["Metadata"] = metadata
        kwargs["MetadataDirective"] = "REPLACE"
    if tags is not NOTHING:
        kwargs["Tagging"] = encode_url_query(tags)
        kwargs["TaggingDirective"] = "REPLACE"

    kwargs.update(
        dict(
            ACL=acl,
            CacheControl=cache_control,
            ContentDisposition=content_disposition,
            ContentEncoding=content_encoding,
            ContentLanguage=content_language,
            ContentMD5=content_md5,
            ContentType=content_type,
            CopySourceIfMatch=copy_source_if_match,
            CopySourceIfModifiedSince=copy_source_if_modified_since,
            CopySourceIfNoneMatch=copy_source_if_none_match,
            CopySourceIfUnmodifiedSince=copy_source_if_unmodified_since,
            Expires=expires_datetime,
            GrantFullControl=grant_full_control,
            GrantRead=grant_read,
            GrantReadACP=grant_read_acp,
            GrantWriteACP=grant_write_acp,
            ServerSideEncryption=server_side_encryption,
            StorageClass=storage_class,
            WebsiteRedirectLocation=website_redirect_location,
            SSECustomerAlgorithm=sse_customer_algorithm,
            SSECustomerKey=sse_customer_key,
            SSEKMSKeyId=sse_kms_key_id,
            SSEKMSEncryptionContext=sse_kms_encryption_context,
            BucketKeyEnabled=bucket_key_enabled,
            CopySourceSSECustomerAlgorithm=copy_source_sse_customer_algorithm,
            CopySourceSSECustomerKey=copy_source_sse_customer_key,
            RequestPayer=request_payer,
            ObjectLockMode=object_lock_mode,
            ObjectLockRetainUntilDate=object_lock_retain_until_datetime,
            ObjectLockLegalHoldStatus=object_lock_legal_hold_status,
            ExpectedBucketOwner=expected_bucket_owner,
            ExpectedSourceBucketOwner=expected_source_bucket_owner,
        )
    )
    return s3_client.copy_object(**resolve_kwargs(**kwargs))
