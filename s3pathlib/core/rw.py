# -*- coding: utf-8 -*-

"""
Read and write related API.

.. _bsm: https://github.com/aws-samples/boto-session-manager-project
.. _get_object: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_object
.. _put_object: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.put_object
.. _decode: https://docs.python.org/3/library/stdtypes.html#bytes.decode
.. _encode: https://docs.python.org/3/library/stdtypes.html#str.encode
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
        version_id: str = NOTHING,
        if_match: str = NOTHING,
        if_modified_since: datetime = NOTHING,
        if_none_match: str = NOTHING,
        if_unmodified_since: datetime = NOTHING,
        range: str = NOTHING,
        response_cache_control: str = NOTHING,
        response_content_disposition: str = NOTHING,
        response_content_encoding: str = NOTHING,
        response_content_language: str = NOTHING,
        response_content_type: str = NOTHING,
        response_expires: str = NOTHING,
        sse_customer_algorithm: str = NOTHING,
        sse_customer_key: str = NOTHING,
        request_payer: str = NOTHING,
        part_number: int = NOTHING,
        expected_bucket_owner: str = NOTHING,
        checksum_mode: str = NOTHING,
        bsm: T.Optional["BotoSesManager"] = None,
    ) -> bytes:
        """
        Read binary data from s3 object.
        A simple wrapper around get_object_.

        :param version_id: See get_object_.
        :param if_match: See get_object_.
        :param if_modified_since: See get_object_.
        :param if_none_match: See get_object_.
        :param if_unmodified_since: See get_object_.
        :param range: See get_object_.
        :param response_cache_control: See get_object_.
        :param response_content_disposition: See get_object_.
        :param response_content_encoding: See get_object_.
        :param response_content_language: See get_object_.
        :param response_content_type: See get_object_.
        :param response_expires: See get_object_.
        :param sse_customer_algorithm: See get_object_.
        :param sse_customer_key: See get_object_.
        :param request_payer: See get_object_.
        :param part_number: See get_object_.
        :param expected_bucket_owner: See get_object_.
        :param checksum_mode: See get_object_.
        :param bsm: See bsm_.

        .. versionadded:: 1.0.3

        .. versionchanged:: 1.1.2

            automatically store metadata in cache.
            
        .. versionchanged:: 2.1.1
        
            add ``version_id`` parameter, and now support full list of get_object_
            arguments.
        """
        s3_client = resolve_s3_client(context, bsm)
        response = s3_client.get_object(
            **resolve_kwargs(
                Bucket=self.bucket,
                Key=self.key,
                VersionId=version_id,
                IfMatch=if_match,
                IfModifiedSince=if_modified_since,
                IfNoneMatch=if_none_match,
                IfUnmodifiedSince=if_unmodified_since,
                Range=range,
                ResponseCacheControl=response_cache_control,
                ResponseContentDisposition=response_content_disposition,
                ResponseContentEncoding=response_content_encoding,
                ResponseContentLanguage=response_content_language,
                ResponseContentType=response_content_type,
                ResponseExpires=response_expires,
                SSECustomerAlgorithm=sse_customer_algorithm,
                SSECustomerKey=sse_customer_key,
                RequestPayer=request_payer,
                PartNumber=part_number,
                ExpectedBucketOwner=expected_bucket_owner,
                ChecksumMode=checksum_mode,
            ),
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
        version_id: str = NOTHING,
        if_match: str = NOTHING,
        if_modified_since: datetime = NOTHING,
        if_none_match: str = NOTHING,
        if_unmodified_since: datetime = NOTHING,
        range: str = NOTHING,
        response_cache_control: str = NOTHING,
        response_content_disposition: str = NOTHING,
        response_content_encoding: str = NOTHING,
        response_content_language: str = NOTHING,
        response_content_type: str = NOTHING,
        response_expires: str = NOTHING,
        sse_customer_algorithm: str = NOTHING,
        sse_customer_key: str = NOTHING,
        request_payer: str = NOTHING,
        part_number: int = NOTHING,
        expected_bucket_owner: str = NOTHING,
        checksum_mode: str = NOTHING,
        bsm: T.Optional["BotoSesManager"] = None,
    ) -> str:
        """
        Read text data from s3 object.
        A simple wrapper around get_object_.

        :param encoding: See decode_.
        :param errors: See decode_.
        :param version_id: See get_object_.
        :param if_match: See get_object_.
        :param if_modified_since: See get_object_.
        :param if_none_match: See get_object_.
        :param if_unmodified_since: See get_object_.
        :param range: See get_object_.
        :param response_cache_control: See get_object_.
        :param response_content_disposition: See get_object_.
        :param response_content_encoding: See get_object_.
        :param response_content_language: See get_object_.
        :param response_content_type: See get_object_.
        :param response_expires: See get_object_.
        :param sse_customer_algorithm: See get_object_.
        :param sse_customer_key: See get_object_.
        :param request_payer: See get_object_.
        :param part_number: See get_object_.
        :param expected_bucket_owner: See get_object_.
        :param checksum_mode: See get_object_.
        :param bsm: See bsm_.

        .. versionadded:: 1.0.3

        .. versionchanged:: 1.1.2

            automatically store metadata in cache.

        .. versionchanged:: 2.1.1

            add ``version_id`` parameter, and now support full list of get_object_
            arguments.
        """
        data = self.read_bytes(
            version_id=version_id,
            if_match=if_match,
            if_modified_since=if_modified_since,
            if_none_match=if_none_match,
            if_unmodified_since=if_unmodified_since,
            range=range,
            response_cache_control=response_cache_control,
            response_content_disposition=response_content_disposition,
            response_content_encoding=response_content_encoding,
            response_content_language=response_content_language,
            response_content_type=response_content_type,
            response_expires=response_expires,
            sse_customer_algorithm=sse_customer_algorithm,
            sse_customer_key=sse_customer_key,
            request_payer=request_payer,
            part_number=part_number,
            expected_bucket_owner=expected_bucket_owner,
            checksum_mode=checksum_mode,
            bsm=bsm,
        )
        return data.decode(encoding, errors=errors)

    def write_bytes(
        self: "S3Path",
        data: bytes,
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
        bsm: T.Optional["BotoSesManager"] = None,
    ) -> "PutObjectOutputTypeDef":
        """
        Write binary data to s3 object.
        A simple wrapper around put_object_.

        :param data: the text you want to write.
        :param metadata: the s3 object metadata in string key value pair dict.
        :param tags: the s3 object tags in string key value pair dict.
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
        :param bsm: See bsm_.

        .. versionadded:: 1.0.3

        .. versionchanged:: 1.1.1

            add ``metadata`` and ``tags`` parameters.
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
        del response["ResponseMetadata"]
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
        bsm: T.Optional["BotoSesManager"] = None,
    ) -> "PutObjectOutputTypeDef":
        """
        Write text to s3 object.
        A simple wrapper around put_object_.

        :param data: the text you want to write.
        :param encoding: See encode_.
        :param errors: See encode_.
        :param metadata: the s3 object metadata in string key value pair dict.
        :param tags: the s3 object tags in string key value pair dict.
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
        :param bsm: See bsm_.

        .. versionadded:: 1.0.3

        .. versionchanged:: 1.1.1

            add ``metadata`` and ``tags`` parameters.
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

        :param data: the text you want to write.
        :param metadata: the s3 object metadata in string key value pair dict.
        :param tags: the s3 object tags in string key value pair dict.
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
        :param bsm: See bsm_.

        .. versionadded:: 1.0.6

        .. versionchanged:: 1.2.1

            add ``metadata`` and ``tags`` parameters.
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

        .. versionadded:: 1.0.6
        """
        if not self.is_dir():
            raise ValueError(f"{self.uri} is not a directory, you cannot make dir!")

        s3_client = resolve_s3_client(context, bsm)
        response = head_object(
            s3_client=s3_client,
            bucket=self.bucket,
            key=self.key,
            ignore_not_found=True,
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
