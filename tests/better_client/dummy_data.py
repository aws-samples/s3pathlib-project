# -*- coding: utf-8 -*-

from s3pathlib.utils import smart_join_s3_key
from s3pathlib.tests.mock import BaseTest
from s3pathlib.tests.paths import (
    dir_test_list_objects_folder,
    dir_test_upload_dir_folder,
)


class DummyData(BaseTest):
    prefix_dummy_data: str
    key_hello: str
    key_soft_folder: str
    prefix_soft_folder: str
    key_soft_folder_file: str
    key_hard_folder: str
    prefix_hard_folder: str
    key_hard_folder_file: str
    key_empty_hard_folder: str
    prefix_empty_hard_folder: str

    key_never_exists: str
    prefix_never_exists: str

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
        - ``/{prefix}/never_exists`` (not exists, just for testing)
        - ``/{prefix}/never_exists/`` (not exists, just for testing)
        """
        s3_client = cls.bsm.s3_client

        cls.prefix_dummy_data = smart_join_s3_key(
            parts=[cls.get_prefix(), "dummy_data"],
            is_dir=True,
        )
        cls.key_hello = smart_join_s3_key(
            parts=[cls.prefix_dummy_data, "hello.txt"],
            is_dir=False,
        )
        cls.key_soft_folder = smart_join_s3_key(
            [cls.prefix_dummy_data, "soft_folder"], is_dir=False
        )
        cls.prefix_soft_folder = cls.key_soft_folder + "/"
        cls.key_soft_folder_file = smart_join_s3_key(
            parts=[cls.prefix_soft_folder, "file.txt"],
            is_dir=False,
        )
        cls.key_hard_folder = smart_join_s3_key(
            parts=[cls.prefix_dummy_data, "hard_folder"],
            is_dir=False,
        )
        cls.prefix_hard_folder = cls.key_hard_folder + "/"
        cls.key_hard_folder_file = smart_join_s3_key(
            parts=[cls.prefix_hard_folder, "file.txt"],
            is_dir=False,
        )
        cls.key_empty_hard_folder = smart_join_s3_key(
            parts=[cls.prefix_dummy_data, "empty_hard_folder"],
            is_dir=False,
        )
        cls.prefix_empty_hard_folder = cls.key_empty_hard_folder + "/"
        cls.key_never_exists = smart_join_s3_key(
            parts=[cls.prefix_dummy_data, "never_exists"],
            is_dir=False,
        )
        cls.prefix_never_exists = cls.key_never_exists + "/"

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

    prefix_test_list_objects: str

    @classmethod
    def setup_list_objects_folder(cls):
        from s3pathlib.better_client.upload import upload_dir

        # setup test data for iter_objects
        cls.prefix_test_list_objects = smart_join_s3_key(
            parts=[cls.get_prefix(), "test_iter_objects"],
            is_dir=True,
        )
        upload_dir(
            s3_client=cls.bsm.s3_client,
            bucket=cls.get_bucket(),
            prefix=cls.prefix_test_list_objects,
            local_dir=f"{dir_test_list_objects_folder}",
            pattern="**/*.txt",
            overwrite=True,
        )

    prefix_test_upload_dir: str

    @classmethod
    def setup_test_upload_dir(cls):
        from s3pathlib.better_client.upload import upload_dir

        cls.prefix_test_upload_dir = smart_join_s3_key(
            parts=[cls.get_prefix(), "test_upload_dir"],
            is_dir=True,
        )
        upload_dir(
            s3_client=cls.bsm.s3_client,
            bucket=cls.get_bucket(),
            prefix=cls.prefix_test_upload_dir,
            local_dir=f"{dir_test_upload_dir_folder}",
            pattern="**/*.txt",
            overwrite=True,
        )
