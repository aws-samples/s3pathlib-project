# -*- coding: utf-8 -*-

from s3pathlib.core import S3Path
from s3pathlib.tests import run_cov_test


class TestComparisonAPIMixin:
    def test_comparison_and_hash(self):
        """
        Test comparison operator

        - ``==``
        - ``!=``
        - ``>``
        - ``<``
        - ``>=``
        - ``<=``
        - ``hash(S3Path())``
        """
        p1 = S3Path("bucket", "file.txt")
        p2 = S3Path("bucket", "folder/")
        p3 = S3Path("bucket")
        p4 = S3Path()
        p5 = S3Path("bucket", "file.txt").relative_to(S3Path("bucket"))
        p6 = S3Path("bucket", "folder/").relative_to(S3Path("bucket"))
        p7 = S3Path("bucket").relative_to(S3Path("bucket"))
        p8 = S3Path.make_relpath("file.txt")
        p9 = S3Path.make_relpath("folder/")

        p_list = [p1, p2, p3, p4, p5, p6, p7, p8, p9]
        for p in p_list:
            assert p == p

        assert p1 != p2
        assert p5 != p6
        assert p4 == p7
        assert p5 == p8
        assert p6 == p9

        assert p1 > p3
        assert p1 >= p3

        assert p3 < p1
        assert p3 <= p1

        assert p2 > p3
        assert p2 >= p3

        assert p3 < p2
        assert p3 <= p2

        p_set = set(p_list + p_list)
        assert len(p_set) == 6
        for p in p_list:
            assert p in p_set


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.core.comparison", open_browser=False)
