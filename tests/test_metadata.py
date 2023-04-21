# -*- coding: utf-8 -*-

import pytest
from s3pathlib.metadata import warn_upper_case_in_metadata_key


def test_warn_upper_case_in_metadata_key():
    with pytest.warns(UserWarning):
        warn_upper_case_in_metadata_key(metadata={"Hello": "World"})


if __name__ == "__main__":
    from s3pathlib.tests import run_cov_test

    run_cov_test(__file__, module="s3pathlib.metadata", preview=False)
