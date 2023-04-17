# -*- coding: utf-8 -*-

from s3pathlib.utils import smart_join_s3_key
from s3pathlib.tests.mock import prefix, BaseTest


class DummyData(BaseTest):
    key_hello: str
    key_soft_folder: str
    prefix_soft_folder: str
    key_soft_folder_file: str
    key_hard_folder: str
    prefix_hard_folder: str
    key_hard_folder_file: str
    key_empty_hard_folder: str
    prefix_empty_hard_folder: str

    @classmethod
    def setup_dummy_data(cls):
        """
        Following test S3 objects are created. Key endswith "/" are special
        empty S3 object representing a folder.

        - ``/{prefix}/hello.txt``
        - ``/{prefix}/soft_folder`` (not exists, just for testing)
        - ``/{prefix}/soft_folder/`` (not exists, just for testing)
        - ``/{prefix}/soft_folder/file.txt``
        - ``/{prefix}/hard_folder`` (not exists, just for testing)
        - ``/{prefix}/hard_folder/``
        - ``/{prefix}/hard_folder/file.txt``
        - ``/{prefix}/empty_hard_folder`` (not exists, just for testing)
        - ``/{prefix}/empty_hard_folder/``
        """
        s3_client = cls.bsm.s3_client

        cls.key_hello = smart_join_s3_key(
            parts=[prefix, "hello.txt"],
            is_dir=False,
        )
        cls.key_soft_folder = smart_join_s3_key([prefix, "soft_folder"], is_dir=False)
        cls.prefix_soft_folder = cls.key_soft_folder + "/"
        cls.key_soft_folder_file = smart_join_s3_key(
            parts=[cls.prefix_soft_folder, "file.txt"],
            is_dir=False,
        )
        cls.key_hard_folder = smart_join_s3_key(
            parts=[prefix, "hard_folder"],
            is_dir=False,
        )
        cls.prefix_hard_folder = cls.key_hard_folder + "/"
        cls.key_hard_folder_file = smart_join_s3_key(
            parts=[cls.prefix_hard_folder, "file.txt"],
            is_dir=False,
        )
        cls.key_empty_hard_folder = smart_join_s3_key(
            parts=[prefix, "empty_hard_folder"],
            is_dir=False,
        )
        cls.prefix_empty_hard_folder = cls.key_empty_hard_folder + "/"

        bucket = cls.get_bucket()

        s3_client.put_object(
            Bucket=bucket,
            Key=cls.key_hello,
            Body="Hello World!",
        )

        s3_client.put_object(
            Bucket=bucket,
            Key=cls.key_soft_folder_file,
            Body="Hello World!",
        )

        s3_client.put_object(
            Bucket=bucket,
            Key=cls.prefix_hard_folder,
            Body="",
        )

        s3_client.put_object(
            Bucket=bucket,
            Key=cls.key_hard_folder_file,
            Body="Hello World!",
        )

        s3_client.put_object(
            Bucket=bucket,
            Key=cls.prefix_empty_hard_folder,
            Body="",
        )
