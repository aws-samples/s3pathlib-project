S3 Object Filtering
==============================================================================

.. contents::
    :class: this-will-duplicate-information-and-it-is-still-useful-here
    :depth: 1
    :local:

In file system, recursively finding files under a directory that matchs certain / combination of criteria is a common use case. It is not easy to do with native AWS API.

``s3pathlib`` provides a Pythonic way to do that. The :meth:`~s3pathlib.core.S3Path.iter_objects`` actually returns an :class:`~s3pathlib.core.S3PathIterProxy` object. It serves as a iterator that yield :class:`~s3pathlib.core.S3Path` items and provides more utility method to control / filter the items you want to return.

.. note::

    When using filter with ``limit`` argument. The iterator yield ``limit`` number of items first, then apply filter to the results afterwards. In conclusion, the final number of matched items is usually SMALLER than ``limit``.


Yield all objects without Filtering
------------------------------------------------------------------------------
.. code-block:: python

    # search scope is entire bucket
    p_dir = S3Path("bucket")
    for p in p_dir.iter_objects():
        ...


Filter by Attribute Value
------------------------------------------------------------------------------
.. code-block:: python

    # only .csv file
    for p in p_dir.iter_objects().filter(S3Path.ext == ".csv"):
        ...

    # only >= 100 MB
    for p in p_dir.iter_objects().filter(S3Path.size >= 100_000_000):
        ...
        

Multiple Filter (logic AND)
------------------------------------------------------------------------------
.. code-block:: python

    # two filter arguments
    for p in p_dir.iter_objects().filter(S3Path.ext == ".csv", S3Path.size >= 100_000_000):
        ...

    # chained filter
    for p in p_dir.iter_objects().filter(S3Path.ext == ".csv").filter(S3Path.size >= 100_000_000):
        ...

List of attributes can be used for filter:

- :meth:`~s3pathlib.core.S3Path.bucket`
- :meth:`~s3pathlib.core.S3Path.key`
- :meth:`~s3pathlib.core.S3Path.uri`
- :meth:`~s3pathlib.core.S3Path.console_url`
- :meth:`~s3pathlib.core.S3Path.arn`
- :meth:`~s3pathlib.core.S3Path.parts`
- :meth:`~s3pathlib.core.S3Path.basename`
- :meth:`~s3pathlib.core.S3Path.fname`
- :meth:`~s3pathlib.core.S3Path.ext`
- :meth:`~s3pathlib.core.S3Path.dirname`
- :meth:`~s3pathlib.core.S3Path.dirpath`
- :meth:`~s3pathlib.core.S3Path.abspath`
- :meth:`~s3pathlib.core.S3Path.etag`
- :meth:`~s3pathlib.core.S3Path.size`
- :meth:`~s3pathlib.core.S3Path.last_modified_at`
- :meth:`~s3pathlib.core.S3Path.version_id`
- :meth:`~s3pathlib.core.S3Path.expire_at`


Custom Filter
------------------------------------------------------------------------------
A filter function is simply a callable function that takes only one argument :class:`S3Path`, and returns a boolean value to indicate that whether we WANT TO KEEP THIS OBJECT. If returns ``False``, this ``S3Path`` will not be yield. You can define arbitrary criterion in your filter function.

Example:

.. code-block:: python

    # the size in bytes is odd number
    def size_is_odd(s3path: S3Path) -> bool:
        return s3path.size % 2

    for p in p_dir.iter_objects().filter(size_is_odd):
        ...


Built in Comparator
------------------------------------------------------------------------------
Example:

.. code-block:: python

    # between lower ~ upper
    for p in p_dir.iter_objects().filter(S3Path.size.between(10_000_000, 50_000_000):
        ...

    # startswith prefix sub string, will match ../log1.txt, ../log2.txt, etc ...
    for p in p_dir.iter_objects().filter(S3Path.basename.startswith("log")):
        ...

    # contains sub string, will match business-report/2022-01-01.pptx
    for p in p_dir.iter_objects().filter(S3Path.abspath.contains("report")):
        ...

List of built-in comparator for filtering:

- :meth:`~s3pathlib.core.FilterableProperty.equal_to`
- :meth:`~s3pathlib.core.FilterableProperty.not_equal_to`
- :meth:`~s3pathlib.core.FilterableProperty.greater`
- :meth:`~s3pathlib.core.FilterableProperty.less`
- :meth:`~s3pathlib.core.FilterableProperty.greater_equal`
- :meth:`~s3pathlib.core.FilterableProperty.less_equal`
- :meth:`~s3pathlib.core.FilterableProperty.between`
- :meth:`~s3pathlib.core.FilterableProperty.startswith`
- :meth:`~s3pathlib.core.FilterableProperty.endswith`
- :meth:`~s3pathlib.core.FilterableProperty.contains`


Control Returned Items
------------------------------------------------------------------------------
Examples:

.. code-block:: python

    >>> root = S3Path("bucket") # assume you have a lots of files in this bucket
    >>> iterproxy = root.iter_objects().filter(
    ...     S3Path.size >= 10_000_000, S3Path.ext == ".csv" # add filter
    ... )

    >>> iterproxy.one() # fetch one
    S3Path('s3://bucket/larger-than-10MB-1.csv')

    >>> iterproxy.many(3) # fetch three
    [
        S3Path('s3://bucket/larger-than-10MB-1.csv'),
        S3Path('s3://bucket/larger-than-10MB-2.csv'),
        S3Path('s3://bucket/larger-than-10MB-3.csv'),
    ]

    >>> for p in iterproxy: # iter the rest
    ...     print(p)

See also:

- :meth:`~s3pathlib.iterproxy.IterProxy.one`:
- :meth:`~s3pathlib.iterproxy.IterProxy.one_or_none`:
- :meth:`~s3pathlib.iterproxy.IterProxy.many`:
- :meth:`~s3pathlib.iterproxy.IterProxy.all`:
- :meth:`~s3pathlib.iterproxy.IterProxy.skip`:
