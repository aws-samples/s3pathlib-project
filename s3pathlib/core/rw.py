# -*- coding: utf-8 -*-

"""
Read and write related API.

.. _put_object: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.put_object
"""

import typing as T
from datetime import datetime

from func_args import NOTHING, resolve_kwargs

from ..type import TagType, MetadataType
from ..tag import encode_url_query
from ..aws import context
from ..better_client.head_object import head_object

from .resolve_s3_client import resolve_s3_client

if T.TYPE_CHECKING:  # pragma: no cover
    from .s3path import S3Path
    from boto_session_manager import BotoSesManager
    from mypy_boto3_s3.type_defs import (
        PutObjectOutputTypeDef,
    )


class ReadAndWriteAPIMixin:
    """
    A mixin class that implements the Text / Bytes, Read / Write methods.
    """

    def read_bytes(
        self: "S3Path",
        bsm: T.Optional["BotoSesManager"] = None,
    ) -> bytes:
        """
        Read binary data from s3 object. A simple wrapper around
        `s3_client.get_object <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_object>`_

        .. versionchanged:: 1.1.2

            automatically store metadata in cache.
        """
        s3_client = resolve_s3_client(context, bsm)
        response = s3_client.get_object(
            Bucket=self.bucket,
            Key=self.key,
        )
        data = response["Body"].read()
        del response["Body"]
        del response["ResponseMetadata"]
        self._meta = response
        return data

    def read_text(
        self: "S3Path",
        encoding="utf-8",
        errors="strict",
        bsm: T.Optional["BotoSesManager"] = None,
    ) -> str:
        """
        Read text data from s3 object. A simple wrapper around s3_client.get_object_

        .. versionchanged:: 1.1.2

            automatically store metadata in cache.

        .. _get_object: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_object
        """
        data = self.read_bytes(bsm=bsm)
        return data.decode(encoding, errors=errors)

    def write_bytes(
        self: "S3Path",
        data: bytes,
        metadata: MetadataType = NOTHING,
        tags: TagType = NOTHING,
        bsm: T.Optional["BotoSesManager"] = None,
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
    ) -> "PutObjectOutputTypeDef":
        """
        Write binary data to s3 object. A simple wrapper around
        `s3_client.put_object <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.put_object>`_

        :param data: the text you want to write.
        :param metadata: the s3 object metadata in string key value pair dict.
        :param tags: the s3 object tags in string key value pair dict.
        :param bsm: ``boto_session_manager.BotoSesManager`` object.
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

        .. versionadded:: 1.0.3

        .. versionchanged:: 1.1.1

            allow update metadata and tags as well
        """
        s3_client = resolve_s3_client(context, bsm)
        response = s3_client.put_object(
            **resolve_kwargs(
                Bucket=self.bucket,
                Key=self.key,
                Body=data,
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
        self._meta = response
        if metadata is not NOTHING:
            self._meta["Metadata"] = metadata
        return response

    def write_text(
        self: "S3Path",
        data: str,
        encoding: str = "utf-8",
        errors: str = "strict",
        metadata: MetadataType = NOTHING,
        tags: TagType = NOTHING,
        bsm: T.Optional["BotoSesManager"] = None,
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
    ) -> "PutObjectOutputTypeDef":
        """
        Write text to s3 object. A simple wrapper around
        `s3_client.put_object <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.put_object>`_

        :param data: the text you want to write.
        :param encoding: how do you want to encode text?
        :param errors: how do you want to handle encode error? can be 'strict',
            ``ignore``, ``replace``, ``xmlcharrefreplace``, ``backslashreplace``.
            see more details `here <https://docs.python.org/3/library/stdtypes.html#str.encode>`_.
        :param metadata: the s3 object metadata in string key value pair dict.
        :param tags: the s3 object tags in string key value pair dict.

        .. versionadded:: 1.0.3

        .. versionchanged:: 1.1.1

            allow update metadata and tags as well
        """
        body = data.encode(encoding, errors=errors)
        return self.write_bytes(
            data=body,
            metadata=metadata,
            tags=tags,
            bsm=bsm,
            acl=acl,
            cache_control=cache_control,
            content_disposition=content_disposition,
            content_encoding=content_encoding,
            content_language=content_language,
            content_length=content_length,
            content_md5=content_md5,
            content_type=content_type,
            checksum_algorithm=checksum_algorithm,
            checksum_crc32=checksum_crc32,
            checksum_crc32c=checksum_crc32c,
            checksum_sha1=checksum_sha1,
            checksum_sha256=checksum_sha256,
            expires_datetime=expires_datetime,
            grant_full_control=grant_full_control,
            grant_read=grant_read,
            grant_read_acp=grant_read_acp,
            grant_write_acp=grant_write_acp,
            server_side_encryption=server_side_encryption,
            storage_class=storage_class,
            website_redirect_location=website_redirect_location,
            sse_customer_algorithm=sse_customer_algorithm,
            sse_customer_key=sse_customer_key,
            sse_kms_key_id=sse_kms_key_id,
            sse_kms_encryption_context=sse_kms_encryption_context,
            bucket_key_enabled=bucket_key_enabled,
            request_payer=request_payer,
            object_lock_mode=object_lock_mode,
            object_lock_retain_until_datetime=object_lock_retain_until_datetime,
            object_lock_legal_hold_status=object_lock_legal_hold_status,
            expected_bucket_owner=expected_bucket_owner,
        )

    def touch(
        self: "S3Path",
        exist_ok: bool = True,
        metadata: MetadataType = NOTHING,
        tags: TagType = NOTHING,
        bsm: T.Optional["BotoSesManager"] = None,
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
    ):
        """
        Create an empty S3 object at the S3 location if the S3 object not exists.
        Do nothing if already exists.

        :param exist_ok: if True, it won't raise error when the S3 object
            already exists.

        .. versionchanged:: 1.0.6

        .. versionchanged:: 1.2.1
        """
        self.ensure_object()

        if self.exists(bsm=bsm):
            if exist_ok:
                pass
            else:
                raise FileExistsError
        else:
            self.write_text(
                "",
                metadata=metadata,
                tags=tags,
                bsm=bsm,
                acl=acl,
                cache_control=cache_control,
                content_disposition=content_disposition,
                content_encoding=content_encoding,
                content_language=content_language,
                content_length=content_length,
                content_md5=content_md5,
                content_type=content_type,
                checksum_algorithm=checksum_algorithm,
                checksum_crc32=checksum_crc32,
                checksum_crc32c=checksum_crc32c,
                checksum_sha1=checksum_sha1,
                checksum_sha256=checksum_sha256,
                expires_datetime=expires_datetime,
                grant_full_control=grant_full_control,
                grant_read=grant_read,
                grant_read_acp=grant_read_acp,
                grant_write_acp=grant_write_acp,
                server_side_encryption=server_side_encryption,
                storage_class=storage_class,
                website_redirect_location=website_redirect_location,
                sse_customer_algorithm=sse_customer_algorithm,
                sse_customer_key=sse_customer_key,
                sse_kms_key_id=sse_kms_key_id,
                sse_kms_encryption_context=sse_kms_encryption_context,
                bucket_key_enabled=bucket_key_enabled,
                request_payer=request_payer,
                object_lock_mode=object_lock_mode,
                object_lock_retain_until_datetime=object_lock_retain_until_datetime,
                object_lock_legal_hold_status=object_lock_legal_hold_status,
                expected_bucket_owner=expected_bucket_owner,
            )

    def mkdir(
        self: "S3Path",
        exist_ok: bool = False,
        parents: bool = False,
        bsm: T.Optional["BotoSesManager"] = None,
    ):
        """
        Make an S3 folder (empty "/" file)

        .. versionchanged:: 1.0.6
        """
        if not self.is_dir():
            raise ValueError(f"{self.uri} is not a directory, you cannot make dir!")

        s3_client = resolve_s3_client(context, bsm)
        response = head_object(
            s3_client=s3_client,
            bucket=self.bucket,
            key=self.key,
        )
        if response is None:
            s3_client.put_object(
                Bucket=self.bucket,
                Key=self.key,
                Body="",
            )
        else:
            if exist_ok:
                pass
            else:
                raise FileExistsError(f"{self.uri} already exists!")

        if parents:
            for p in self.parents:
                if p.is_bucket() is False:
                    p.mkdir(exist_ok=True, parents=False, bsm=bsm)
