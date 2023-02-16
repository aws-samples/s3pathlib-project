# -*- coding: utf-8 -*-

"""
Copy file from s3 to s3.
"""

import typing as T
from datetime import datetime

from func_args import NOTHING

from .. import client as better_client
from ..type import TagType, MetadataType

from .resolve_s3_client import resolve_s3_client
from ..aws import context

if T.TYPE_CHECKING:  # pragma: no cover
    from .s3path import S3Path
    from boto_session_manager import BotoSesManager


class CopyAPIMixin:
    """
    A mixin class that implements copy method.
    """

    def copy_file(
        self: "S3Path",
        dst: "S3Path",
        metadata: T.Optional[MetadataType] = NOTHING,
        tags: T.Optional[TagType] = NOTHING,
        overwrite: bool = False,
        bsm: T.Optional["BotoSesManager"] = None,
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
        Copy an S3 directory to a different S3 directory, including all
        sub-directory and files.

        :param dst: copy to s3 object, it has to be an object
        :param overwrite: if False, none of the file will be uploaded / overwritten
            if any of target s3 location already taken.

        :return: number of object are copied, 0 or 1.

        .. versionadded:: 1.0.1

        .. versionchanged:: 1.3.1

            add ``metadata`` and ``tags`` argument
        """
        self.ensure_object()
        dst.ensure_object()
        self.ensure_not_relpath()
        dst.ensure_not_relpath()
        if overwrite is False:
            dst.ensure_not_exists(bsm=bsm)
        s3_client = resolve_s3_client(context, bsm)
        return better_client.copy_object(
            s3_client,
            src_bucket=self.bucket,
            src_key=self.key,
            dst_bucket=dst.bucket,
            dst_key=dst.key,
            metadata=metadata,
            tags=tags,
            acl=acl,
            cache_control=cache_control,
            content_disposition=content_disposition,
            content_encoding=content_encoding,
            content_language=content_language,
            content_md5=content_md5,
            content_type=content_type,
            copy_source_if_match=copy_source_if_match,
            copy_source_if_modified_since=copy_source_if_modified_since,
            copy_source_if_none_match=copy_source_if_none_match,
            copy_source_if_unmodified_since=copy_source_if_unmodified_since,
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
            copy_source_sse_customer_algorithm=copy_source_sse_customer_algorithm,
            copy_source_sse_customer_key=copy_source_sse_customer_key,
            request_payer=request_payer,
            object_lock_mode=object_lock_mode,
            object_lock_retain_until_datetime=object_lock_retain_until_datetime,
            object_lock_legal_hold_status=object_lock_legal_hold_status,
            expected_bucket_owner=expected_bucket_owner,
            expected_source_bucket_owner=expected_source_bucket_owner,
        )

    def copy_dir(
        self: "S3Path",
        dst: "S3Path",
        metadata: T.Optional[MetadataType] = NOTHING,
        tags: T.Optional[TagType] = NOTHING,
        overwrite: bool = False,
        bsm: T.Optional["BotoSesManager"] = None,
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
    ):
        """
        Copy an S3 directory to a different S3 directory, including all
        sub-directory and files.

        :param dst: copy to s3 directory, it has to be a directory
        :param overwrite: if False, none of the file will be uploaded / overwritten
            if any of target s3 location already taken.

        :return: number of objects are copied

        .. versionadded:: 1.0.1
        """
        self.ensure_dir()
        dst.ensure_dir()
        self.ensure_not_relpath()
        dst.ensure_not_relpath()
        todo: T.List[T.Tuple["S3Path", "S3Path"]] = list()
        for p_src in self.iter_objects(bsm=bsm):
            p_relpath = p_src.relative_to(self)
            p_dst = dst.joinpath(p_relpath)
            todo.append((p_src, p_dst))

        if overwrite is False:
            for p_src, p_dst in todo:
                p_dst.ensure_not_exists(bsm=bsm)

        for p_src, p_dst in todo:
            p_src.copy_file(
                p_dst,
                metadata=metadata,
                tags=tags,
                overwrite=True,
                bsm=bsm,
                acl=acl,
                cache_control=cache_control,
                content_disposition=content_disposition,
                content_encoding=content_encoding,
                content_language=content_language,
                content_md5=content_md5,
                content_type=content_type,
                copy_source_if_match=copy_source_if_match,
                copy_source_if_modified_since=copy_source_if_modified_since,
                copy_source_if_none_match=copy_source_if_none_match,
                copy_source_if_unmodified_since=copy_source_if_unmodified_since,
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
                copy_source_sse_customer_algorithm=copy_source_sse_customer_algorithm,
                copy_source_sse_customer_key=copy_source_sse_customer_key,
                request_payer=request_payer,
                object_lock_mode=object_lock_mode,
                object_lock_retain_until_datetime=object_lock_retain_until_datetime,
                object_lock_legal_hold_status=object_lock_legal_hold_status,
                expected_bucket_owner=expected_bucket_owner,
                expected_source_bucket_owner=expected_source_bucket_owner,
            )

        return len(todo)

    def copy_to(
        self: "S3Path",
        dst: "S3Path",
        metadata: T.Optional[MetadataType] = NOTHING,
        tags: T.Optional[TagType] = NOTHING,
        overwrite: bool = False,
        bsm: T.Optional["BotoSesManager"] = None,
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
    ) -> int:
        """
        Copy s3 object or s3 directory from one place to another place.

        :param dst: copy to s3 path
        :param overwrite: if False, non of the file will be upload / overwritten
            if any of target s3 location already taken.

        .. versionadded:: 1.0.1

        .. versionchanged:: 1.3.1

            add ``metadata`` and ``tags`` argument
        """
        if self.is_dir():
            return self.copy_dir(
                dst=dst,
                metadata=metadata,
                tags=tags,
                overwrite=overwrite,
                bsm=bsm,
                acl=acl,
                cache_control=cache_control,
                content_disposition=content_disposition,
                content_encoding=content_encoding,
                content_language=content_language,
                content_md5=content_md5,
                content_type=content_type,
                copy_source_if_match=copy_source_if_match,
                copy_source_if_modified_since=copy_source_if_modified_since,
                copy_source_if_none_match=copy_source_if_none_match,
                copy_source_if_unmodified_since=copy_source_if_unmodified_since,
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
                copy_source_sse_customer_algorithm=copy_source_sse_customer_algorithm,
                copy_source_sse_customer_key=copy_source_sse_customer_key,
                request_payer=request_payer,
                object_lock_mode=object_lock_mode,
                object_lock_retain_until_datetime=object_lock_retain_until_datetime,
                object_lock_legal_hold_status=object_lock_legal_hold_status,
                expected_bucket_owner=expected_bucket_owner,
                expected_source_bucket_owner=expected_source_bucket_owner,
            )
        elif self.is_file():
            self.copy_file(
                dst=dst,
                overwrite=overwrite,
                bsm=bsm,
                metadata=metadata,
                tags=tags,
                acl=acl,
                cache_control=cache_control,
                content_disposition=content_disposition,
                content_encoding=content_encoding,
                content_language=content_language,
                content_md5=content_md5,
                content_type=content_type,
                copy_source_if_match=copy_source_if_match,
                copy_source_if_modified_since=copy_source_if_modified_since,
                copy_source_if_none_match=copy_source_if_none_match,
                copy_source_if_unmodified_since=copy_source_if_unmodified_since,
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
                copy_source_sse_customer_algorithm=copy_source_sse_customer_algorithm,
                copy_source_sse_customer_key=copy_source_sse_customer_key,
                request_payer=request_payer,
                object_lock_mode=object_lock_mode,
                object_lock_retain_until_datetime=object_lock_retain_until_datetime,
                object_lock_legal_hold_status=object_lock_legal_hold_status,
                expected_bucket_owner=expected_bucket_owner,
                expected_source_bucket_owner=expected_source_bucket_owner,
            )
            return 1
        else:  # pragma: no cover
            raise TypeError

    def move_to(
        self: "S3Path",
        dst: "S3Path",
        metadata: T.Optional[MetadataType] = NOTHING,
        tags: T.Optional[TagType] = NOTHING,
        overwrite: bool = False,
        bsm: T.Optional["BotoSesManager"] = None,
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
    ) -> int:
        """
        Move s3 object or s3 directory from one place to another place. It is
        firstly :meth:`S3Path.copy_to` then :meth:`S3Path.delete_if_exists`

        :param dst: copy to s3 path
        :param overwrite: if False, non of the file will be upload / overwritten
            if any of target s3 location already taken.

        .. versionadded:: 1.0.1

        .. versionchanged:: 1.3.1

            add ``metadata`` and ``tags`` argument
        """
        count = self.copy_to(
            dst=dst,
            metadata=metadata,
            tags=tags,
            overwrite=overwrite,
            bsm=bsm,
            acl=acl,
            cache_control=cache_control,
            content_disposition=content_disposition,
            content_encoding=content_encoding,
            content_language=content_language,
            content_md5=content_md5,
            content_type=content_type,
            copy_source_if_match=copy_source_if_match,
            copy_source_if_modified_since=copy_source_if_modified_since,
            copy_source_if_none_match=copy_source_if_none_match,
            copy_source_if_unmodified_since=copy_source_if_unmodified_since,
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
            copy_source_sse_customer_algorithm=copy_source_sse_customer_algorithm,
            copy_source_sse_customer_key=copy_source_sse_customer_key,
            request_payer=request_payer,
            object_lock_mode=object_lock_mode,
            object_lock_retain_until_datetime=object_lock_retain_until_datetime,
            object_lock_legal_hold_status=object_lock_legal_hold_status,
            expected_bucket_owner=expected_bucket_owner,
            expected_source_bucket_owner=expected_source_bucket_owner,
        )
        self.delete_if_exists(bsm=bsm)
        return count
