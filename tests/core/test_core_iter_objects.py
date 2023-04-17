# -*- coding: utf-8 -*-

import pytest

from pathlib_mate import Path
from iterproxy import and_

from s3pathlib.core import S3Path
from s3pathlib.client import put_object
from s3pathlib.tests import run_cov_test
from s3pathlib.tests.mock import BaseTest


dir_here = Path.dir_here(__file__)


class IterObjectsAPIMixin(BaseTest):
    module = "core.iter_objects"
    s3dir_test_iter_objects: S3Path

    @classmethod
    def custom_setup_class(cls):
        # for iter objects
        cls.s3dir_test_iter_objects = (
            cls.get_s3dir_root().joinpath("test_iter_objects").to_dir()
        )
        cls.s3dir_test_iter_objects.delete_if_exists()
        cls.s3dir_test_iter_objects.upload_dir(
            local_dir=dir_here.joinpath("test_iter_objects").abspath,
            pattern="**/*.txt",
            overwrite=True,
        )

    def _test_fetcher_methods(self):
        """
        - one
        - one_or_none
        - many
        - all
        - skip
        """
        # case 1
        proxy = self.s3dir_test_iter_objects.iter_objects()
        assert proxy.one().basename == "README.txt"
        assert proxy.one().basename == "folder-description.txt"

        assert proxy.one_or_none().basename == "1.txt"
        proxy.skip(5)

        l = proxy.many(2)
        assert [p.basename for p in l] == ["7.txt", "8.txt"]

        l = proxy.many(2)
        assert [p.basename for p in l] == [
            "9.txt",
        ]

        assert proxy.all() == []
        assert proxy.one_or_none() is None

        # case 2
        proxy = self.s3dir_test_iter_objects.iter_objects()

        l = proxy.many(5)
        assert [
            p.basename for p in l
        ] == "README.txt,folder-description.txt,1.txt,2.txt,3.txt".split(",")

        l = proxy.all()
        assert [p.basename for p in l] == "4.txt,5.txt,6.txt,7.txt,8.txt,9.txt".split(
            ","
        )

        with pytest.raises(StopIteration):
            proxy.many(3)

    def _test_filter(self):
        proxy = self.s3dir_test_iter_objects.iter_objects().filter(
            and_(S3Path.basename == "1.txt")
        )
        l = proxy.all()
        assert [p.basename for p in l] == [
            "1.txt",
        ]

    def _test_metadata_filter(self):
        # --- operator
        for p in self.s3dir_test_iter_objects.iter_objects().filter(S3Path.size > 30):
            assert p.size > 30
        for p in self.s3dir_test_iter_objects.iter_objects().filter(S3Path.size < 30):
            assert p.size < 30
        for p in self.s3dir_test_iter_objects.iter_objects().filter(S3Path.size >= 30):
            assert p.size >= 30
        for p in self.s3dir_test_iter_objects.iter_objects().filter(S3Path.size <= 30):
            assert p.size <= 30

        etag = "f177b7fa5b1a86a54c7d97fbdad4a61e"
        for p in self.s3dir_test_iter_objects.iter_objects().filter(
            S3Path.etag == etag
        ):
            assert p.etag == etag
        for p in self.s3dir_test_iter_objects.iter_objects().filter(
            S3Path.etag != etag
        ):
            assert p.etag != etag

        # --- method
        for p in self.s3dir_test_iter_objects.iter_objects().filter(
            S3Path.size.between(20, 40)
        ):
            assert 20 <= p.size <= 40

        for p in self.s3dir_test_iter_objects.iter_objects().filter(
            S3Path.basename.startswith("README")
        ):
            assert p.basename.startswith("README")

        for p in self.s3dir_test_iter_objects.iter_objects().filter(
            S3Path.basename.endswith(".txt")
        ):
            assert p.basename.endswith(".txt")

        for p in self.s3dir_test_iter_objects.iter_objects().filter(
            S3Path.dirname.contains("folder")
        ):
            assert "folder" in p.dirname

        for p in self.s3dir_test_iter_objects.iter_objects().filter(
            S3Path.etag.equal_to(etag)
        ):
            assert p.etag == etag

        # --- built-in filter
        with pytest.raises(ValueError):
            self.s3dir_test_iter_objects.iter_objects().filter_by_ext()

        for p in self.s3dir_test_iter_objects.iter_objects().filter_by_ext(".TXT"):
            assert p.ext.lower() == ".txt"

        for p in self.s3dir_test_iter_objects.iter_objects().filter_by_ext(
            ".txt", ".TXT"
        ):
            assert p.ext.lower() == ".txt"

    def _test_iterdir(self):
        p_list = self.s3dir_test_iter_objects.iterdir().all()
        assert len(p_list) == 5
        assert p_list[0].is_dir()
        assert p_list[-1].is_file()

        p_list = self.s3dir_test_iter_objects.joinpath("fold").iterdir().all()
        assert len(p_list) == 4
        assert p_list[0].is_dir()
        assert p_list[-1].is_file()
        assert p_list[-1].basename == "folder-description.txt"

    def _test_statistics(self):
        s3dir_statistics = self.s3dir_root.joinpath("statistics").to_dir()

        for i in range(1, 1 + 4):
            self.s3_client.put_object(
                Bucket=s3dir_statistics.bucket,
                Key=S3Path(s3dir_statistics, "folder1", f"{i}.txt").key,
                Body="Hello World!",
            )

        for i in range(1, 1 + 6):
            self.s3_client.put_object(
                Bucket=s3dir_statistics.bucket,
                Key=S3Path(s3dir_statistics, "folder2", f"{i}.json").key,
                Body='{"message": "Hello World!"}',
            )

        assert s3dir_statistics.count_objects() == 10

        count, total_size = s3dir_statistics.calculate_total_size()
        assert count == 10
        assert total_size == 210

        count, total_size = s3dir_statistics.calculate_total_size(for_human=True)
        assert count == 10
        assert total_size == "210 B"

    def _test_count_objects(self):
        s3path_soft_folder_file = self.s3dir_root.joinpath("soft_folder", "file.txt")
        put_object(
            self.s3_client,
            s3path_soft_folder_file.bucket,
            s3path_soft_folder_file.key,
            b"a",
        )

        s3dir_hard_folder = self.s3dir_root.joinpath("hard_folder").to_dir()
        put_object(
            self.s3_client,
            s3dir_hard_folder.bucket,
            s3dir_hard_folder.key,
            b"",
        )

        s3path_hard_folder_file = self.s3dir_root.joinpath("hard_folder", "file.txt")
        put_object(
            self.s3_client,
            s3path_hard_folder_file.bucket,
            s3path_hard_folder_file.key,
            b"a",
        )

        s3dir_empty_folder = self.s3dir_root.joinpath("empty_folder").to_dir()
        put_object(
            self.s3_client,
            s3dir_empty_folder.bucket,
            s3dir_empty_folder.key,
            b"",
        )

        # soft / hard / empty folder
        assert s3path_soft_folder_file.parent.count_objects() == 1
        assert s3dir_hard_folder.count_objects() == 1
        assert s3dir_empty_folder.count_objects() == 0

    def test(self):
        self._test_fetcher_methods()
        self._test_filter()
        self._test_metadata_filter()
        self._test_iterdir()
        self._test_statistics()
        self._test_count_objects()


class Test(IterObjectsAPIMixin):
    use_mock = False


class TestWithVersioning(IterObjectsAPIMixin):
    use_mock = True


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.core.iter_objects", preview=False)
