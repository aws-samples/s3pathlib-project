# -*- coding: utf-8 -*-

import pytest
from s3pathlib import exc


def test_ensure_one_and_only_one_not_null():
    value_error_cases = [
        dict(),
        dict(a=None, b=None),
        dict(a=1, b=1),
    ]
    good_cases = [
        dict(a=None, b=1),
        dict(a=1, b=None),
        dict(a=None, b=None, c=1),
    ]
    for kwargs in value_error_cases:
        with pytest.raises(ValueError):
            exc.ensure_one_and_only_one_not_none(**kwargs)
    for kwargs in good_cases:
        exc.ensure_one_and_only_one_not_none(**kwargs)


def test_ensure_all_none():
    value_error_cases = [
        dict(),
        dict(a=None, b=1),
        dict(a=1, b=None),
        dict(a=1, b=1),
    ]
    good_cases = [
        dict(a=None),
        dict(a=None, b=None),
    ]
    for kwargs in value_error_cases:
        with pytest.raises(ValueError):
            exc.ensure_all_none(**kwargs)
    for kwargs in good_cases:
        exc.ensure_all_none(**kwargs)


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
