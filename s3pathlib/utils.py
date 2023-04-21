# -*- coding: utf-8 -*-

import typing as T
import hashlib

try:
    import botocore.exceptions
except ImportError:  # pragma: no cover
    pass
except:  # pragma: no cover
    raise


def split_s3_uri(
    s3_uri: str,
) -> T.Tuple[str, str]:
    """
    Split AWS S3 URI, returns bucket and key.

    :param s3_uri: example, ``"s3://my-bucket/my-folder/data.json"``

    .. versionadded:: 1.0.1
    """
    parts = s3_uri.split("/")
    bucket = parts[2]
    key = "/".join(parts[3:])
    return bucket, key


def join_s3_uri(
    bucket: str,
    key: str,
) -> str:
    """
    Join AWS S3 URI from bucket and key.

    :param bucket: example, ``"my-bucket"``
    :param key: example, ``"my-folder/data.json"`` or ``"my-folder/"``

    .. versionadded:: 1.0.1
    """
    return "s3://{}/{}".format(bucket, key)


def split_parts(key: str) -> T.List[str]:
    """
    Split s3 key parts using "/" delimiter.

    Example::

        >>> split_parts("a/b/c")
        ["a", "b", "c"]
        >>> split_parts("//a//b//c//")
        ["a", "b", "c"]

    .. versionadded:: 1.0.1
    """
    return [part for part in key.split("/") if part]


def smart_join_s3_key(
    parts: T.List[str],
    is_dir: bool,
) -> str:
    """
    Note, it assume that there's no such double slack in your path. It ensure
    that there's only one consecutive "/" in the s3 key.

    :param parts: list of s3 key path parts, could have "/"
    :param is_dir: if True, the s3 key ends with "/". otherwise enforce no
        tailing "/".

    Example::

        >>> smart_join_s3_key(parts=["/a/", "b/", "/c"], is_dir=True)
        a/b/c/
        >>> smart_join_s3_key(parts=["/a/", "b/", "/c"], is_dir=False)
        a/b/c

    .. versionadded:: 1.0.1
    """
    new_parts = list()
    for part in parts:
        new_parts.extend(split_parts(part))
    key = "/".join(new_parts)
    if is_dir:
        return key + "/"
    else:
        return key


def make_s3_console_url(
    bucket: T.Optional[str] = None,
    prefix: T.Optional[str] = None,
    s3_uri: T.Optional[str] = None,
    version_id: T.Optional[str] = None,
    is_us_gov_cloud: bool = False,
) -> str:
    """
    Return an AWS Console url that you can use to open it in your browser.

    :param bucket: example, ``"my-bucket"``
    :param prefix: example, ``"my-folder/"``
    :param s3_uri: example, ``"s3://my-bucket/my-folder/data.json"``

    Example::

        >>> make_s3_console_url(s3_uri="s3://my-bucket/my-folder/data.json")
        https://s3.console.aws.amazon.com/s3/object/my-bucket?prefix=my-folder/data.json

    .. versionadded:: 1.0.1

    .. versionchanged:: 2.0.1

        add ``version_id`` parameter.
    """
    if s3_uri is None:
        if not ((bucket is not None) and (prefix is not None)):
            raise ValueError
    else:
        if not ((bucket is None) and (prefix is None)):
            raise ValueError
        bucket, prefix = split_s3_uri(s3_uri)

    if len(prefix) == 0:
        return "https://console.aws.amazon.com/s3/buckets/{}?tab=objects".format(
            bucket,
        )
    elif prefix.endswith("/"):
        s3_type = "buckets"
        prefix_part = f"prefix={prefix}"
    else:
        s3_type = "object"
        prefix_part = f"prefix={prefix}"

    if is_us_gov_cloud:
        endpoint = "console.amazonaws-us-gov.com"
    else:
        endpoint = "console.aws.amazon.com"

    if version_id is None:
        version_part = ""
    else:
        version_part = f"&versionId={version_id}"

    return (
        f"https://{endpoint}/s3/{s3_type}/{bucket}?{prefix_part}{version_part}"
    )


def make_s3_select_console_url(
    bucket: str,
    key: str,
    is_us_gov_cloud: bool,
) -> str:
    if is_us_gov_cloud:
        endpoint = "console.amazonaws-us-gov.com"
    else:
        endpoint = "console.aws.amazon.com"
    return "https://{endpoint}/s3/buckets/{bucket}/object/select?prefix={key}".format(
        endpoint=endpoint,
        bucket=bucket,
        key=key,
    )


def ensure_s3_object(
    s3_key_or_uri: str,
) -> None:
    """
    Raise exception if the string is not in valid format for a AWS S3 object

    .. versionadded:: 1.0.1
    """
    if s3_key_or_uri.endswith("/"):
        raise ValueError("'{}' doesn't represent s3 object!".format(s3_key_or_uri))


def ensure_s3_dir(
    s3_key_or_uri: str
) -> None:
    """
    Raise exception if the string is not in valid format for a AWS S3 directory

    .. versionadded:: 1.0.1
    """
    if not s3_key_or_uri.endswith("/"):
        raise ValueError("'{}' doesn't represent s3 dir!".format(s3_key_or_uri))


def validate_s3_bucket(bucket):
    """
    Ref:
    https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
    """
    pass


def validate_s3_key(key):
    """
    Ref:
    https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html#object-key-guidelines
    """
    pass


MAGNITUDE_OF_DATA = {
    i: v
    for i, v in enumerate(["B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"])
}


def repr_data_size(
    size_in_bytes: int,
    precision: int = 2,
) -> str:  # pragma: no cover
    """
    Return human readable string represent of a file size. Doesn't support
    size greater than 1YB.

    For example:

    - 100 bytes => 100 B
    - 100,000 bytes => 97.66 KB
    - 100,000,000 bytes => 95.37 MB
    - 100,000,000,000 bytes => 93.13 GB
    - 100,000,000,000,000 bytes => 90.95 TB
    - 100,000,000,000,000,000 bytes => 88.82 PB
    - and more ...

    Magnitude of data::

        1000         kB    kilobyte
        1000 ** 2    MB    megabyte
        1000 ** 3    GB    gigabyte
        1000 ** 4    TB    terabyte
        1000 ** 5    PB    petabyte
        1000 ** 6    EB    exabyte
        1000 ** 7    ZB    zettabyte
        1000 ** 8    YB    yottabyte

    .. versionadded:: 1.0.1
    """
    if size_in_bytes < 1024:
        return "%s B" % size_in_bytes

    index = 0
    while 1:
        index += 1
        size_in_bytes, mod = divmod(size_in_bytes, 1024)
        if size_in_bytes < 1024:
            break
    template = "{0:.%sf} {1}" % precision
    s = template.format(size_in_bytes + mod / 1024.0, MAGNITUDE_OF_DATA[index])
    return s


def parse_data_size(s) -> int:  # pragma: no cover
    """
    Parse human readable string representing a file size. Doesn't support
    size greater than 1YB.

    Examples::

        >>> parse_data_size("3.43 MB")
        3596615

        >>> parse_data_size("2_512.4 MB")
        2634442342

        >>> parse_data_size("2,512.4 MB")
        2634442342

    .. versionadded:: 1.0.5
    """
    s = s.strip()

    # split digits and
    digits = set("01234567890_,.")
    digit_parts = list()
    ind = 0
    for ind, c in enumerate(s):
        if c in digits:
            digit_parts.append(c)
        else:
            break
    digit = "".join(digit_parts)
    digit = digit.replace("_", "").replace(",", "")
    digit = float(digit)

    unit_part = s[ind:].strip()

    unit_ind = None
    for ind, unit in MAGNITUDE_OF_DATA.items():
        if unit_part.upper() == unit:
            unit_ind = ind
            break

    if unit_ind is None:
        raise ValueError

    unit = 1024 ** unit_ind
    return int(digit * unit)


def hash_binary(
    b: bytes,
    hash_meth: callable,
) -> str:  # pragma: no cover
    """
    Get the hash of a binary object.

    :param b: binary object
    :param hash_meth: callable hash method, example: hashlib.md5

    :return: hash value in hex digits.

    .. versionadded:: 1.0.1
    """
    m = hash_meth()
    m.update(b)
    return m.hexdigest()


def md5_binary(
    b: bytes,
) -> str:  # pragma: no cover
    """
    Get the md5 hash of a binary object.

    :param b: binary object

    :return: hash value in hex digits.

    .. versionadded:: 1.0.1
    """
    return hash_binary(b, hashlib.md5)


def sha256_binary(
    b: bytes,
) -> str:  # pragma: no cover
    """
    Get the md5 hash of a binary object.

    :param b: binary object

    :return: hash value in hex digits.

    .. versionadded:: 1.0.1
    """
    return hash_binary(b, hashlib.sha256)


DEFAULT_CHUNK_SIZE = 1 << 6


def hash_file(
    abspath: str,
    hash_meth: callable,
    nbytes: int = 0,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> str:  # pragma: no cover
    """
    Get the hash of a file on local drive.

    :param abspath: absolute path of the file
    :param hash_meth: callable hash method, example: hashlib.md5
    :param nbytes: only hash first nbytes of the file
    :param chunk_size: internal option, stream chunk_size of the data for hash
        each time, avoid high memory usage.

    :return: hash value in hex digits.

    .. versionadded:: 1.0.1
    """
    if nbytes < 0:
        raise ValueError("nbytes cannot smaller than 0")
    if chunk_size < 1:
        raise ValueError("nbytes cannot smaller than 1")
    if (nbytes > 0) and (nbytes < chunk_size):
        chunk_size = nbytes

    m = hash_meth()
    with open(abspath, "rb") as f:
        if nbytes:  # use first n bytes
            have_reads = 0
            while True:
                have_reads += chunk_size
                if have_reads > nbytes:
                    n = nbytes - (have_reads - chunk_size)
                    if n:
                        data = f.read(n)
                        m.update(data)
                    break
                else:
                    data = f.read(chunk_size)
                    m.update(data)
        else:  # use entire content
            while True:
                data = f.read(chunk_size)
                if not data:
                    break
                m.update(data)

    return m.hexdigest()


def grouper_list(
    l: T.Iterable,
    n: int,
) -> T.Iterable[list]:  # pragma: no cover
    """
    Evenly divide list into fixed-length piece, no filled value if chunk
    size smaller than fixed-length.

    Example::

        >>> list(grouper_list(range(10), n=3)
        [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]

    :param l: an iterable object
    :param n: number of item per list

    .. versionadded:: 1.0.1
    """
    chunk = list()
    counter = 0
    for item in l:
        counter += 1
        chunk.append(item)
        if counter == n:
            yield chunk
            chunk = list()
            counter = 0
    if len(chunk) > 0:
        yield chunk
