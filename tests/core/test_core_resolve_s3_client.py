# -*- coding: utf-8 -*-

from s3pathlib.aws import context
from s3pathlib.tests import bsm, boto_ses
from s3pathlib.core.resolve_s3_client import resolve_s3_client


def test_resolve_s3_client():
    context.attach_boto_session(boto_ses=boto_ses)
    assert context.aws_account_id == bsm.aws_account_id
    assert context.aws_region == bsm.aws_region
    s3_client_1 = resolve_s3_client(context, None)
    s3_client_2 = resolve_s3_client(context, bsm)
    assert id(s3_client_1) != id(s3_client_2)


if __name__ == "__main__":
    from s3pathlib.tests import run_cov_test

    run_cov_test(__file__, module="s3pathlib.core.resolve_s3_client", open_browser=False)
