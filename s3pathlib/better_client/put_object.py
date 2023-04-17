# -*- coding: utf-8 -*-

import typing as T
from datetime import datetime

from func_args import NOTHING, resolve_kwargs

from ..type import TagType, MetadataType
from ..tag import encode_url_query

if T.TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_s3.type_defs import PutObjectOutputTypeDef


def put_object(
    s3_client: "S3Client",
    bucket: str,
    key: str,
    body: T.Union[bytes, str] = NOTHING,
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
) -> T.Union[dict, "PutObjectOutputTypeDef"]:
    """
    wrapper of put_object_.

    :param s3_client: See put_object_.
    :param bucket: See put_object_.
    :param key: See put_object_.
    :param body: See put_object_.
    :param metadata: See put_object_.
    :param tags: a Python dict key value pair.
    :param acl: See put_object_.
    :param cache_control: See put_object_.
    :param content_disposition: See put_object_.
    :param content_encoding: See put_object_.
    :param content_language: See put_object_.
    :param content_length: See put_object_.
    :param content_md5: See put_object_.
    :param content_type: See put_object_.
    :param checksum_algorithm: See put_object_.
    :param checksum_crc32: See put_object_.
    :param checksum_crc32c: See put_object_.
    :param checksum_sha1: See put_object_.
    :param checksum_sha256: See put_object_.
    :param expires_datetime: See put_object_.
    :param grant_full_control: See put_object_.
    :param grant_read: See put_object_.
    :param grant_read_acp: See put_object_.
    :param grant_write_acp: See put_object_.
    :param server_side_encryption: See put_object_.
    :param storage_class: See put_object_.
    :param website_redirect_location: See put_object_.
    :param sse_customer_algorithm: See put_object_.
    :param sse_customer_key: See put_object_.
    :param sse_kms_key_id: See put_object_.
    :param sse_kms_encryption_context: See put_object_.
    :param bucket_key_enabled: See put_object_.
    :param request_payer: See put_object_.
    :param object_lock_mode: See put_object_.
    :param object_lock_retain_until_datetime: See put_object_.
    :param object_lock_legal_hold_status: See put_object_.
    :param expected_bucket_owner: See put_object_.

    .. _put_object: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.put_object
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