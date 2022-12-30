# -*- coding: utf-8 -*-

"""
Copy file from s3 to s3.
"""

import typing as T

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
        metadata: T.Optional[MetadataType] = None,
        tags: T.Optional[TagType] = None,
        overwrite: bool = False,
        bsm: T.Optional["BotoSesManager"] = None,
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
        )

    def copy_dir(
        self: "S3Path",
        dst: "S3Path",
        overwrite: bool = False,
        bsm: T.Optional["BotoSesManager"] = None,
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
            p_src.copy_file(p_dst, overwrite=True, bsm=bsm)

        return len(todo)

    def copy_to(
        self: "S3Path",
        dst: "S3Path",
        metadata: T.Optional[MetadataType] = None,
        tags: T.Optional[TagType] = None,
        overwrite: bool = False,
        bsm: T.Optional["BotoSesManager"] = None,
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
                overwrite=overwrite,
                bsm=bsm,
            )
        elif self.is_file():
            self.copy_file(
                dst=dst,
                overwrite=overwrite,
                bsm=bsm,
                metadata=metadata,
                tags=tags,
            )
            return 1
        else:  # pragma: no cover
            raise TypeError

    def move_to(
        self: "S3Path",
        dst: "S3Path",
        metadata: T.Optional[MetadataType] = None,
        tags: T.Optional[TagType] = None,
        overwrite: bool = False,
        bsm: T.Optional["BotoSesManager"] = None,
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
        )
        self.delete_if_exists(bsm=bsm)
        return count
