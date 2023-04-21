# -*- coding: utf-8 -*-

"""
.. _upload_file: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/upload_file.html#
"""

import typing as T

from pathlib_mate import Path

from .. import exc
from ..type import PathType
from ..utils import join_s3_uri
from .head_object import is_object_exists


if T.TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_s3 import S3Client


def upload_dir(
    s3_client: "S3Client",
    bucket: str,
    prefix: str,
    local_dir: PathType,
    pattern: str = "**/*",
    overwrite: bool = False,
) -> int:
    """
    Recursively upload a local directory and files in its subdirectory to S3.

    :param s3_client: A ``boto3.session.Session().client("s3")`` object.
    :param bucket: S3 bucket name.
    :param prefix: The s3 prefix (logic directory) you want to upload to.
    :param local_dir: Absolute path of the directory on the local
        file system you want to upload.
    :param pattern: Linux styled glob pattern match syntax. see this official
        guide https://docs.python.org/3/library/pathlib.html#pathlib.Path.glob
        for more details.
    :param overwrite: If False, none of the file will be uploaded / overwritten
        if any of target s3 location already taken.

    :return: number of files uploaded

    .. versionadded:: 1.0.1
    """
    # preprocess input arguments
    if prefix.endswith("/"):
        prefix = prefix[:-1]

    p_local_dir = Path(local_dir)

    if p_local_dir.is_file():
        raise TypeError(f"'{p_local_dir}' is a file, not a directory!")

    if p_local_dir.exists() is False:
        raise FileNotFoundError(f"'{p_local_dir}' not found!")

    if len(prefix):
        final_prefix = f"{prefix}/"
    else:
        final_prefix = ""

    # list of (local file path, target s3 key)
    todo: T.List[T.Tuple[str, str]] = list()
    for p in p_local_dir.glob(pattern):
        if p.is_file():
            relative_path = p.relative_to(p_local_dir)
            key = "{}{}".format(final_prefix, "/".join(relative_path.parts))
            todo.append((p.abspath, key))

    # make sure all target s3 location not exists
    if overwrite is False:
        for abspath, key in todo:
            if is_object_exists(s3_client, bucket, key) is True:
                s3_uri = join_s3_uri(bucket, key)
                raise exc.S3FileAlreadyExist.make(s3_uri)

    # execute upload
    for abspath, key in todo:
        s3_client.upload_file(abspath, bucket, key)

    return len(todo)
