# -*- coding: utf-8 -*-

"""
Is the S3Path a XYZ testing.

- :meth:`~IsTestAPIMixin.is_void`
- :meth:`~IsTestAPIMixin.is_dir`
- :meth:`~IsTestAPIMixin.is_file`
- :meth:`~IsTestAPIMixin.is_bucket`
"""

import typing as T

if T.TYPE_CHECKING:  # pragma: no cover
    from .s3path import S3Path


class IsTestAPIMixin:
    """
    A mixin class that implements the condition test methods.
    """

    def is_void(self: "S3Path") -> bool:
        """
        Test if it is a void S3 path.

        **Definition**

        no bucket, no key, no parts, no type, no nothing.
        A void path is also a special :meth:`relative path <is_relpath>`,
        because any path join with void path results to itself.
        """
        return (self._bucket is None) and (len(self._parts) == 0)

    def is_dir(self: "S3Path") -> bool:
        """
        Test if it is a S3 directory

        **Definition**

        A logical S3 directory that never physically exists. An S3
        :meth:`bucket <is_bucket>` is also a special dir, which is the root dir.

        .. versionadded:: 1.0.1
        """
        if self._is_dir is None:
            return False
        else:
            return self._is_dir

    def is_file(self: "S3Path") -> bool:
        """
        Test if it is a S3 object

        **Definition**

        A S3 object.

        .. versionadded:: 1.0.1
        """
        if self._is_dir is None:
            return False
        else:
            return not self._is_dir

    def is_bucket(self: "S3Path") -> bool:
        """
        Test if it is a S3 bucket.

        **Definition**

        A S3 bucket, the root folder S3 path is equivalent to a S3 bucket.
        A S3 bucket are always :meth:`is_dir() is True <is_dir>`

        .. versionadded:: 1.0.1
        """
        return (
            (self._bucket is not None)
            and (len(self._parts) == 0)
            and (self._is_dir is True)
        )

    def ensure_object(self: "S3Path") -> None:
        """
        A validator method that ensure it represents a S3 object.

        .. versionadded:: 1.0.1
        """
        if self.is_file() is not True:
            raise TypeError(f"S3 URI: {self} IS NOT a valid s3 object!")

    def ensure_file(self: "S3Path") -> None:
        """
        A validator method that ensure it represents a S3 object.

        .. versionadded:: 1.2.1
        """
        return self.ensure_object()

    def ensure_not_object(self: "S3Path") -> None:
        """
        A validator method that ensure it IS NOT a S3 object.

        .. versionadded:: 1.2.1
        """
        if self.is_file() is True:
            raise TypeError(f"S3 URI: {self} IS an s3 object!")

    def ensure_not_file(self: "S3Path") -> None:
        """
        A validator method that ensure it IS NOT a S3 object.

        .. versionadded:: 1.2.1
        """
        self.ensure_not_object()

    def ensure_dir(self: "S3Path") -> None:
        """
        A validator method that ensure it represents a S3 dir.

        .. versionadded:: 1.0.1
        """
        if self.is_dir() is not True:
            raise TypeError(f"{self} IS NOT a valid s3 directory!")

    def ensure_not_dir(self: "S3Path") -> None:
        """
        A validator method that ensure it IS NOT a S3 dir.

        .. versionadded:: 1.2.1
        """
        if self.is_dir() is True:
            raise TypeError(f"{self} IS a s3 directory!")
