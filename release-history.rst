.. _release_history:

Release and Version History
==============================================================================


1.0.12 (TODO)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- :class:`s3pathlib.aws.Context` object is now singleton.
- allow ``and_``, ``or_``, ``not_`` in iterproxy filter.
- add ``tagging`` management feature
- add ``acl`` management feature
- add ``legal_hold`` management feature

**Minor Improvements**

**Bugfixes**

**Miscellaneous**


1.0.11 (TODO)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- add the ``__truediv__`` operator override. it is a ``s3path / part1 / part2`` syntax sugar.
- add the ``__sub__`` operator override. it is a ``S3Path("bucket/folder") - S3Path("bucket")`` syntax sugar.

**Minor Improvements**

**Bugfixes**

**Miscellaneous**


1.0.10 (2022-04-30)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Miscellaneous**

- remove the boto session module, now it depends on `boto_session_manager <https://pypi.org/project/boto-session-manager/>`_ library.
- add compatibility support for smart_open >= 6.0 due to the ``ignore_ext`` arg is removed.
- for s3 IO feature, you need ``smart_open>=5.1.x``


1.0.9 (2022-04-19)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- add :meth:`s3pathlib.core.S3Path.boto_ses.BotoSesManager.get_client` method


1.0.8 (2022-04-19)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Bugfixes**

-  fix import bug in :mod:`s3pathlib.aws` module


1.0.7 (2022-04-17)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Bugfixes**

-  fix import bug in :mod:`s3pathlib.boto_ses` module


1.0.6 (2022-04-13)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- add :meth:`s3pathlib.core.S3Path.to_file` method.
- add :meth:`s3pathlib.core.S3Path.to_dir` method.
- add :meth:`s3pathlib.core.S3Path.parents` method.
- add :meth:`s3pathlib.core.S3Path.iterdir` method.
- add :meth:`s3pathlib.core.S3Path.touch` method.
- add :meth:`s3pathlib.core.S3Path.mkdir` method.
- add :class:`s3pathlib.core.S3Path.boto_ses.BotoSesManager` class.

**Minor Improvements**

- add ``bsm`` boto session manager parameter for all method using s3 api.

**Bugfixes**

**Miscellaneous**


1.0.5 (Planned)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- add :meth:`s3pathlib.core.S3Path.us_gov_cloud_console_url` property
- add :func:`s3pathlib.utils.parse_data_size` method

**Minor Improvements**

**Bugfixes**

**Miscellaneous**


1.0.4 (Planned)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- add :meth:`s3pathlib.core.S3PathIterProxy.equal_to`
- add :meth:`s3pathlib.core.S3PathIterProxy.not_equal_to`
- add :meth:`s3pathlib.core.S3PathIterProxy.greater`
- add :meth:`s3pathlib.core.S3PathIterProxy.greater_equal`
- add :meth:`s3pathlib.core.S3PathIterProxy.less`
- add :meth:`s3pathlib.core.S3PathIterProxy.less_equal`
- add ``recursive = True | False`` argument for :meth:`s3pathlib.util.iter_objects`, so you can ignore files in nested folders
- add ``recursive = True | False`` argument for :meth:`s3pathlib.core.S3Path.iter_objects`, so you can ignore files in nested folders

**Minor Improvements**

**Bugfixes**

- fix a bug that :meth:`s3pathlib.core.S3Path.fname` was a regular property and not filterable

**Miscellaneous**

- Add "S3 Object filter" doc
- Add "File Liked Object IO Object filter" doc


1.0.3 (2022-01-23)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- make :class:`s3pathlib.core.S3Path` a file-like object that support open, read, write.
- add :class:`s3pathlib.core.S3PathIterProxy` that greatly simplify S3 object filtering.
- add :meth:`s3pathlib.core.S3Path.open` method, makes ``S3Path`` a file-like object
- add :meth:`s3pathlib.core.S3Path.write_text`
- add :meth:`s3pathlib.core.S3Path.read_text`
- add :meth:`s3pathlib.core.S3Path.write_bytes`
- add :meth:`s3pathlib.core.S3Path.read_bytes`


1.0.2 (2022-01-21)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- add :meth:`s3pathlib.core.S3Path.from_s3_uri` method.
- add :meth:`s3pathlib.core.S3Path.from_s3_arn` method.
- add :meth:`s3pathlib.core.S3Path.change` method.
- add :meth:`s3pathlib.core.S3Path.is_parent_of` method.
- add :meth:`s3pathlib.core.S3Path.is_prefix_of` method.
- add :meth:`s3pathlib.core.S3Path.dirpath` property.
- add better support to handle auto-created "empty folder" object, add ``include_folder=True`` parameter for :meth:`s3pathlib.core.S3Path.list_objects`, :meth:`s3pathlib.core.S3Path.count_objects`, :meth:`s3pathlib.core.S3Path.calculate_total_size` method.

**Bugfixes**

- fix a bug that AWS S3 will create an invisible object when creating a folder, it should not counts as a valid object for :meth:`s3pathlib.core.S3Path.count_objects`

**Miscellaneous**

- A lot doc improvement.


1.0.1 (2022-01-19)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- ``s3pathlib.S3Path`` API becomes stable
- ``s3pathlib.utils`` API becomes stable
- ``s3pathlib.context`` API becomes stable

**Miscellaneous**

- First stable release.


0.0.1 (2022-01-17)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- First release, a placeholder release.
