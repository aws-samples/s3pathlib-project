# -*- coding: utf-8 -*-

import pytest
from s3pathlib.aws import context
from s3pathlib.tests import bsm, boto_ses
from s3pathlib.core.base import _resolve_s3_client


def test_resolve_s3_client():
    context.attach_boto_session(boto_ses=boto_ses)
    assert context.aws_account_id == bsm.aws_account_id
    assert context.aws_region == bsm.aws_region
    s3_client_1 = _resolve_s3_client(context, None)
    s3_client_2 = _resolve_s3_client(context, bsm)
    assert id(s3_client_1) != id(s3_client_2)


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
