# -*- coding: utf-8 -*-

import os
import pytest

from s3pathlib import utils
from s3pathlib.tests import s3_client, bucket, prefix

dir_here = os.path.dirname(os.path.abspath(__file__))
dir_tests = dir_here

prefix = utils.smart_join_s3_key(
    parts=[prefix, "utils", "s3_client_enhancement"], is_dir=True,
)


class TestS3ClientEnhancement:
    @classmethod
    def setup_class(cls):
        """
        Following test S3 objects are created. Key endswith "/" are special
        empty S3 object representing a folder.

        - ``/{prefix}/hello.txt``
        - ``/{prefix}/soft_folder/file.txt``
        - ``/{prefix}/hard_folder/``
        - ``/{prefix}/hard_folder/file.txt``
        - ``/{prefix}/empty_hard_folder/``
        """
        s3_client.put_object(
            Bucket=bucket,
            Key=utils.smart_join_s3_key(
                parts=[prefix, "hello.txt"],
                is_dir=False,
            ),
            Body="Hello World!",
        )

        s3_client.put_object(
            Bucket=bucket,
            Key=utils.smart_join_s3_key(
                parts=[prefix, "soft_folder/file.txt"],
                is_dir=False,
            ),
            Body="Hello World!",
        )

        s3_client.put_object(
            Bucket=bucket,
            Key=utils.smart_join_s3_key(
                parts=[prefix, "hard_folder"],
                is_dir=True,
            ),
            Body="",
        )

        s3_client.put_object(
            Bucket=bucket,
            Key=utils.smart_join_s3_key(
                parts=[prefix, "hard_folder/file.txt"],
                is_dir=False,
            ),
            Body="Hello World!",
        )

        s3_client.put_object(
            Bucket=bucket,
            Key=utils.smart_join_s3_key(
                parts=[prefix, "empty_hard_folder"],
                is_dir=True,
            ),
            Body="",
        )

    def test_exists(self):
        assert utils.exists(
            s3_client=s3_client,
            bucket=bucket,
            key=utils.smart_join_s3_key(parts=[prefix, "hello.txt"], is_dir=False)
        ) is True

        assert utils.exists(
            s3_client=s3_client,
            bucket=bucket,
            key=utils.smart_join_s3_key(parts=[prefix, "soft_folder", "file.txt"], is_dir=False)
        ) is True

        assert utils.exists(
            s3_client=s3_client,
            bucket=bucket,
            key=utils.smart_join_s3_key(parts=[prefix, "soft_folder"], is_dir=True)
        ) is False

        assert utils.exists(
            s3_client=s3_client,
            bucket=bucket,
            key=utils.smart_join_s3_key(parts=[prefix, "hard_folder", "file.txt"], is_dir=False)
        ) is True

        assert utils.exists(
            s3_client=s3_client,
            bucket=bucket,
            key=utils.smart_join_s3_key(parts=[prefix, "hard_folder"], is_dir=True)
        ) is True

        assert utils.exists(
            s3_client=s3_client,
            bucket=bucket,
            key=utils.smart_join_s3_key(parts=[prefix, "hard_folder"], is_dir=False)
        ) is False

        assert utils.exists(
            s3_client=s3_client,
            bucket=bucket,
            key=utils.smart_join_s3_key(parts=[prefix, "empty_hard_folder"], is_dir=True)
        ) is True

        assert utils.exists(
            s3_client=s3_client,
            bucket=bucket,
            key=utils.smart_join_s3_key(parts=[prefix, "never_exists"], is_dir=False)
        ) is False

        assert utils.exists(
            s3_client=s3_client,
            bucket=bucket,
            key=utils.smart_join_s3_key(parts=[prefix, "never_exists"], is_dir=True)
        ) is False

    def test_upload_dir(self):
        local_dir = os.path.join(dir_tests, "test_upload_dir")

        # regular upload
        utils.upload_dir(
            s3_client=s3_client,
            bucket=bucket,
            prefix=utils.smart_join_s3_key(parts=[prefix, "test_upload_dir"], is_dir=True),
            local_dir=local_dir,
            pattern="**/*.txt",
            overwrite=True,
        )

        # upload to bucket root directory also works
        utils.upload_dir(
            s3_client=s3_client,
            bucket=bucket,
            prefix="",
            local_dir=local_dir,
            pattern="**/*.txt",
            overwrite=True,
        )

        with pytest.raises(FileExistsError):
            utils.upload_dir(
                s3_client=s3_client,
                bucket=bucket,
                prefix=utils.smart_join_s3_key(parts=[prefix, "test_upload_dir"], is_dir=True),
                local_dir=local_dir,
                pattern="**/*.txt",
                overwrite=False,
            )

    def test_iter_objects(self):
        # setup s3 directory
        utils.upload_dir(
            s3_client=s3_client,
            bucket=bucket,
            prefix=utils.smart_join_s3_key(
                parts=[prefix, "test_iter_objects"], is_dir=True
            ),
            local_dir=os.path.join(dir_tests, "test_iter_objects"),
            pattern="**/*.txt",
            overwrite=True,
        )

        # invalid batch_size
        with pytest.raises(ValueError):
            list(utils.iter_objects(
                s3_client=None, bucket=None, prefix=None,
                batch_size=-1,
            ))

        # invalid batch_size
        with pytest.raises(ValueError):
            list(utils.iter_objects(
                s3_client=None, bucket=None, prefix=None,
                batch_size=9999,
            ))

        # batch_size < limit
        result = list(
            utils.iter_objects(
                s3_client=s3_client,
                bucket=bucket,
                prefix=utils.smart_join_s3_key(
                    parts=[prefix, "test_iter_objects"], is_dir=True
                ),
                batch_size=3,
                limit=5,
            )
        )
        assert len(result) == 5

        # batch_size > limit
        result = list(
            utils.iter_objects(
                s3_client=s3_client,
                bucket=bucket,
                prefix=utils.smart_join_s3_key(
                    parts=[prefix, "test_iter_objects"], is_dir=True
                ),
                batch_size=10,
                limit=3,
            )
        )
        assert len(result) == 3

        # limit >> total number objects,
        result = list(
            utils.iter_objects(
                s3_client=s3_client,
                bucket=bucket,
                prefix=utils.smart_join_s3_key(
                    parts=[prefix, "test_iter_objects"], is_dir=True
                ),
                batch_size=10,
            )
        )
        assert len(result) == 11

        # recursive = False
        result = list(
            utils.iter_objects(
                s3_client=s3_client,
                bucket=bucket,
                prefix=utils.smart_join_s3_key(
                    parts=[prefix, "test_iter_objects"], is_dir=True
                ),
                batch_size=1,
                recursive=False,
                include_folder=False,
            )
        )
        assert len(result) == 2

        result = list(
            utils.iter_objects(
                s3_client=s3_client,
                bucket=bucket,
                prefix=utils.smart_join_s3_key(
                    parts=[prefix, "test_iter_objects", "folder1"], is_dir=True
                ),
                batch_size=10,
                recursive=False,
                include_folder=False,
            )
        )
        assert len(result) == 3

        # recursive = False but it is a file, not a dir
        with pytest.raises(ValueError):
            list(
                utils.iter_objects(
                    s3_client=s3_client,
                    bucket=bucket,
                    prefix=utils.smart_join_s3_key(
                        parts=[prefix, "test_iter_objects", "folder1"], is_dir=False
                    ),
                    batch_size=10,
                    recursive=False,
                    include_folder=True,
                )
            )

    def test_calculate_total_size(self):
        count, total_size = utils.calculate_total_size(
            s3_client=s3_client,
            bucket=bucket,
            prefix=utils.smart_join_s3_key(parts=[prefix, "soft_folder"], is_dir=True),
            include_folder=False,
        )
        assert count == 1

        count, total_size = utils.calculate_total_size(
            s3_client=s3_client,
            bucket=bucket,
            prefix=utils.smart_join_s3_key(parts=[prefix, "soft_folder"], is_dir=True),
            include_folder=True,
        )
        assert count == 1

        count, total_size = utils.calculate_total_size(
            s3_client=s3_client,
            bucket=bucket,
            prefix=utils.smart_join_s3_key(parts=[prefix, "hard_folder"], is_dir=True),
            include_folder=False,
        )
        assert count == 1

        count, total_size = utils.calculate_total_size(
            s3_client=s3_client,
            bucket=bucket,
            prefix=utils.smart_join_s3_key(parts=[prefix, "hard_folder"], is_dir=True),
            include_folder=True,
        )
        assert count == 2

        count, total_size = utils.calculate_total_size(
            s3_client=s3_client,
            bucket=bucket,
            prefix=utils.smart_join_s3_key(parts=[prefix, "empty_hard_folder"], is_dir=True),
            include_folder=False,
        )
        assert count == 0

        count, total_size = utils.calculate_total_size(
            s3_client=s3_client,
            bucket=bucket,
            prefix=utils.smart_join_s3_key(parts=[prefix, "empty_hard_folder"], is_dir=True),
            include_folder=True,
        )
        assert count == 1

        count, total_size = utils.calculate_total_size(
            s3_client=s3_client,
            bucket=bucket,
            prefix=utils.smart_join_s3_key(parts=[prefix, "never_exists"], is_dir=True),
            include_folder=True,
        )
        assert count == 0

    def test_count_objects(self):
        assert utils.count_objects(
            s3_client=s3_client,
            bucket=bucket,
            prefix=utils.smart_join_s3_key(parts=[prefix, "soft_folder"], is_dir=True),
            include_folder=False,
        ) == 1

        assert utils.count_objects(
            s3_client=s3_client,
            bucket=bucket,
            prefix=utils.smart_join_s3_key(parts=[prefix, "soft_folder"], is_dir=True),
            include_folder=True,
        ) == 1

        assert utils.count_objects(
            s3_client=s3_client,
            bucket=bucket,
            prefix=utils.smart_join_s3_key(parts=[prefix, "hard_folder"], is_dir=True),
            include_folder=False,
        ) == 1

        assert utils.count_objects(
            s3_client=s3_client,
            bucket=bucket,
            prefix=utils.smart_join_s3_key(parts=[prefix, "hard_folder"], is_dir=True),
            include_folder=True,
        ) == 2

        assert utils.count_objects(
            s3_client=s3_client,
            bucket=bucket,
            prefix=utils.smart_join_s3_key(parts=[prefix, "empty_hard_folder"], is_dir=True),
            include_folder=False,
        ) == 0

        assert utils.count_objects(
            s3_client=s3_client,
            bucket=bucket,
            prefix=utils.smart_join_s3_key(parts=[prefix, "empty_hard_folder"], is_dir=True),
            include_folder=True,
        ) == 1

        assert utils.count_objects(
            s3_client=s3_client,
            bucket=bucket,
            prefix=utils.smart_join_s3_key(parts=[prefix, "never_exists"], is_dir=True),
            include_folder=True,
        ) == 0

    def test_delete_dir(self):
        # setup s3 directory
        utils.upload_dir(
            s3_client=s3_client,
            bucket=bucket,
            prefix=utils.smart_join_s3_key(parts=[prefix, "test_delete_dir"], is_dir=True),
            local_dir=os.path.join(dir_tests, "test_iter_objects"),
            pattern="**/*.txt",
            overwrite=True,
        )

        # before state
        assert utils.count_objects(
            s3_client=s3_client,
            bucket=bucket,
            prefix=utils.smart_join_s3_key(parts=[prefix, "test_delete_dir"], is_dir=True),
        ) == 11

        assert utils.calculate_total_size(
            s3_client=s3_client,
            bucket=bucket,
            prefix=utils.smart_join_s3_key(parts=[prefix, "test_delete_dir"], is_dir=True),
        )[1] > 0

        # call api
        utils.delete_dir(
            s3_client=s3_client,
            bucket=bucket,
            prefix=utils.smart_join_s3_key(parts=[prefix, "test_delete_dir"], is_dir=True),
        )

        # after state
        assert utils.count_objects(
            s3_client=s3_client,
            bucket=bucket,
            prefix=utils.smart_join_s3_key(parts=[prefix, "test_delete_dir"], is_dir=True),
        ) == 0

        assert utils.calculate_total_size(
            s3_client=s3_client,
            bucket=bucket,
            prefix=utils.smart_join_s3_key(parts=[prefix, "test_delete_dir"], is_dir=True),
        )[1] == 0


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
