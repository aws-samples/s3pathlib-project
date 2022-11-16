# -*- coding: utf-8 -*-

import pytest


def test():
    import s3pathlib

    _ = s3pathlib.S3Path
    _ = s3pathlib.context
    _ = s3pathlib.utils


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
