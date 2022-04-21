.. _release_history:

Release and Version History
==============================================================================


1.0.7 (TODO)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

**Minor Improvements**

**Bugfixes**

**Miscellaneous**


1.0.6 (Planned)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- :class:`s3pathlib.aws.Context` object is now singleton
- allow ``and_``, ``or_``, ``not_`` in iterproxy filter

**Minor Improvements**

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
