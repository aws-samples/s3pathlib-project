import os
import time
import pytest
from s3pathlib.aws import context
from s3pathlib.core import S3Path
from s3pathlib.tests import boto_ses, bucket, prefix

context.attach_boto_session(boto_ses)

dir_here = os.path.dirname(os.path.abspath(__file__))
dir_tests = dir_here


class TestS3Pathlib:
    p = S3Path(bucket, prefix, "iterproxy/")

    @classmethod
    def setup_class(cls):
        cls.p.upload_dir(
            local_dir=os.path.join(dir_tests, "test_iter_objects"),
            pattern="**/*.txt",
            overwrite=True,
        )
        time.sleep(1)

    def test_fetcher_methods(self):
        """
        - one
        - one_or_none
        - many
        - all
        - skip
        """
        # case 1
        proxy = self.p.iter_objects()
        assert proxy.one().basename == "README.txt"

        assert proxy.one_or_none().basename == "1.txt"
        proxy.skip(5)

        l = proxy.many(2)
        assert [p.basename for p in l] == ["7.txt", "8.txt"]

        l = proxy.many(2)
        assert [p.basename for p in l] == ["9.txt", ]

        assert proxy.all() == []
        assert proxy.one_or_none() is None

        # case 2
        proxy = self.p.iter_objects()

        l = proxy.many(4)
        assert [p.basename for p in l] == "README.txt,1.txt,2.txt,3.txt".split(",")

        l = proxy.all()
        assert [p.basename for p in l] == "4.txt,5.txt,6.txt,7.txt,8.txt,9.txt".split(",")

        with pytest.raises(StopIteration):
            proxy.many(3)

    def test_filter(self):
        proxy = self.p.iter_objects().filter(S3Path.basename == "1.txt")
        l = proxy.all()
        assert [p.basename for p in l] == ["1.txt", ]

    def test_metadata_filter(self):
        # --- operator
        for p in self.p.iter_objects().filter(S3Path.size > 30):
            assert p.size > 30
        for p in self.p.iter_objects().filter(S3Path.size < 30):
            assert p.size < 30
        for p in self.p.iter_objects().filter(S3Path.size >= 30):
            assert p.size >= 30
        for p in self.p.iter_objects().filter(S3Path.size <= 30):
            assert p.size <= 30

        etag = "f177b7fa5b1a86a54c7d97fbdad4a61e"
        for p in self.p.iter_objects().filter(S3Path.etag == etag):
            assert p.etag == etag
        for p in self.p.iter_objects().filter(S3Path.etag != etag):
            assert p.etag != etag

        # --- method
        for p in self.p.iter_objects().filter(S3Path.size.between(20, 40)):
            assert 20 <= p.size <= 40

        for p in self.p.iter_objects().filter(S3Path.basename.startswith("README")):
            assert p.basename.startswith("README")

        for p in self.p.iter_objects().filter(S3Path.basename.endswith(".txt")):
            assert p.basename.endswith(".txt")

        for p in self.p.iter_objects().filter(S3Path.dirname.contains("folder")):
            assert "folder" in p.dirname

        for p in self.p.iter_objects().filter(S3Path.etag.equal_to(etag)):
            assert p.etag == etag

        # --- built-in filter
        with pytest.raises(ValueError):
            self.p.iter_objects().filter_by_ext()

        for p in self.p.iter_objects().filter_by_ext(".TXT"):
            assert p.ext.lower() == ".txt"

        for p in self.p.iter_objects().filter_by_ext(".txt", ".TXT"):
            assert p.ext.lower() == ".txt"


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
