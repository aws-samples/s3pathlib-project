# -*- coding: utf-8 -*-

import pytest
from s3pathlib import S3Path


@pytest.fixture
def bucket() -> S3Path:
    return S3Path("bucket")


@pytest.fixture
def directory() -> S3Path:
    return S3Path("bucket", "folder/")


@pytest.fixture
def file() -> S3Path:
    return S3Path("bucket", "folder", "file.txt")


@pytest.fixture
def relpath() -> S3Path:
    return S3Path("bucket", "folder", "file.txt").relative_to(S3Path("bucket"))


@pytest.fixture
def void() -> S3Path:
    return S3Path()
