# -*- coding: utf-8 -*-

from s3pathlib.aws import context
from s3pathlib.core.resolve_s3_client import resolve_s3_client
from s3pathlib.tests import run_cov_test
from s3pathlib.tests.mock import BaseTest


class ResolveS3Client(BaseTest):
    def _test_resolve_s3_client(self):
        context.attach_boto_session(boto_ses=self.bsm.boto_ses)
        assert context.aws_account_id == self.bsm.aws_account_id
        assert context.aws_region == self.bsm.aws_region
        s3_client_1 = resolve_s3_client(context, None)
        s3_client_2 = resolve_s3_client(context, self.bsm)
        assert id(s3_client_1) != id(s3_client_2)

    def test(self):
        self._test_resolve_s3_client()


class Test(ResolveS3Client):
    use_mock = False


class TestUseMock(ResolveS3Client):
    use_mock = True


if __name__ == "__main__":
    run_cov_test(__file__, module="s3pathlib.core.resolve_s3_client", preview=False)
