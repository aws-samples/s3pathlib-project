# -*- coding: utf-8 -*-

import pytest
from s3pathlib.core import S3Path
from s3pathlib import exc
from s3pathlib.better_client.list_objects import (
    ObjectTypeDefIterproxy,
    CommonPrefixTypeDefIterproxy,
    ListObjectsV2OutputTypeDefIterproxy,
    paginate_list_objects_v2,
    filter_object_only,
    calculate_total_size,
    count_objects,
)
from s3pathlib.utils import smart_join_s3_key
from s3pathlib.tests import run_cov_test
from s3pathlib.tests.mock import prefix, BaseTest

from dummy_data import DummyData


class BetterListObjects(DummyData):
    module = "better_client.list_objects"

    @classmethod
    def custom_setup_class(cls):
        cls.setup_dummy_data()

    def _test(self):
        pass

    def test(self):
        self._test()


# class Test(BetterHeadObject):
#     use_mock = False


class TestUseMock(BetterHeadObject):
    use_mock = True


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.better_client.head_object", preview=False)
