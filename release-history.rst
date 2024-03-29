.. _release_history:

Release and Version History
==============================================================================

:orphan:


Backlog (TODO)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- make :class:`s3pathlib.aws.Context` multi-thread safe.


2.0.1 (2023-04-21)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

Changes:

- most of methods now supports ``version_id`` argument for S3 versioning enabled bucket.
- :meth:`s3pathlib.core.rw.ReadAndWriteAPIMixin.read_bytes` and :meth:`s3pathlib.core.rw.ReadAndWriteAPIMixin.read_text` now support ``version_id`` and full list of ``get_object`` boto3 arguments.
- :meth:`s3pathlib.core.uri.UriAPIMixin.console_url` now take the version id into consideration.
- add :meth:`s3pathlib.core.is_test.IsTestAPIMixin.is_delete_marker` method.
- :meth:`s3pathlib.core.base.BaseS3Path.__new__` now can take S3 URI and ARN directly without using the ``from_s3_uri`` and ``from_s3_arn`` method.

New APIs:

- add ``s3pathlib.better_client`` to the public API, it is a collection of more user friendly boto3 API, with type hint.
- add :meth:`~s3pathlib.core.iter_object_versions.IterObjectVersionsAPIMixin.list_object_versions` method. It will return all objects, all versions, all delete markers in reverse chronological order.
- The :meth:`~s3pathlib.core.is_test.IsTestAPIMixin.is_delete_marker` method can be used to check if the version is a delete marker.
- :meth:`~s3pathlib.core.delete.DeleteAPIMixin.delete`: delete object, directory (recursively), specific object version, object versions, all objects all versions in directory. This API will replace the old :meth:`~s3pathlib.core.delete.DeleteAPIMixin.delete_if_exists` API in 3.X.Y.

**Minor Improvements**

- use moto for unit test.
- add tons of more unit test for edge cases.
- prompt to confirm when delete everything in a bucket.

**Bugfixes**

- fix a bug that should not allow user to set value to attributes like ``bucket``, ``key``.

**Miscellaneous**

- completely rewrite the documentation in Jupyter notebook to provide interactive reading experience.

**Imcompatible Change**

- removed :meth:`~s3pathlib.core.joinpath.JoinPathAPIMixin.join_path`.


1.4.1 (2023-02-16)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- now the following API supports full list of native boto3 argument:
    - ``S3Path.write_bytes``
    - ``S3Path.write_text``
    - ``S3Path.copy_file``
    - ``S3Path.copy_dir``
    - ``S3Path.copy_to``
    - ``S3Path.move_to``
- add many optional argument to the ``S3Path.sync()`` method.


1.3.1 (2022-12-30)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- allow ``and_``, ``or_``, ``not_`` in iterproxy filter.
- allow update ``metadata`` and ``taggings`` in :meth:`~s3pathlib.core.S3Path.copy.CopyAPIMixin.copy_file`, :meth:`~s3pathlib.core.S3Path.copy.CopyAPIMixin.copy_to` and :meth:`~s3pathlib.core.S3Path.copy.CopyAPIMixin.move_to` method.

**Miscellaneous**

- the ``iterproxy.py`` module is taken out and released as a independent project.


1.2.1 (2022-11-20)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- :meth:`~s3pathlib.core.opener.OpenerAPIMixin.open` method now takes ``metadata`` and ``tags`` arguments.
- :meth:`~s3pathlib.core.rw.ReadAndWriteAPIMixin.write_bytes` and :meth:`~s3pathlib.core.rw.ReadAndWriteAPIMixin.write_text`, :meth:`~s3pathlib.core.rw.ReadAndWriteAPIMixin.touch` method now takes ``metadata`` and ``tags`` arguments.
- add :meth:`~s3pathlib.core.sync.SyncAPIMixin.sync`, :meth:`~s3pathlib.core.sync.SyncAPIMixin.sync_from`, :meth:`~s3pathlib.core.sync.SyncAPIMixin.sync_to` method that execute `aws s3 sync <https://docs.aws.amazon.com/cli/latest/reference/s3/sync.html>`_ command

**Minor Improvements**

- raise a warning if there is upper case key used in user defined metadata.


1.1.2 (2022-11-16)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Minor Improvements**

- Refactor ``core.py`` module, split the 2.5k line script into 10+ 100 line module.
- Made the metadata pull more intelligent.

**Bugfixes**

- Fix a bug that the ``S3Path`` constructor should not take concrete S3 object / dir path as the second / third / fourth / ... arguments
- Fix a bug that cannot get the metadata value of the S3Path is created by ``_from_content_dict`` method.


1.1.1 (2022-11-13)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- add ``tagging`` management feature
    - add :meth:`~s3pathlib.core.S3Path.get_tags` method
    - add :meth:`~s3pathlib.core.S3Path.put_tags` method
    - add :meth:`~s3pathlib.core.S3Path.update_tags` method
- allow update ``metadata`` and ``taggings`` in :meth:`~s3pathlib.core.S3Path.write_text` and :meth:`~s3pathlib.core.S3Path.write_bytes` method.


1.0.12 (2022-09-10)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- add :meth:`s3pathlib.core.S3Path.joinpath` method to mimick ``pathlib.Path.joinpath`` behavior
- add :meth:`s3pathlib.core.S3Path.s3_select_console_url` property
- add :meth:`s3pathlib.core.S3Path.s3_select_us_gov_cloud_console_url` property

**Bugfixes**

- made :meth:`s3pathlib.core.S3Path.console_url` and :meth:`s3pathlib.core.S3Path.us_gov_cloud_console_url` regular property, they should not be ``FilterableProperty``

**Miscellaneous**

- mark :meth:`s3pathlib.core.S3Path.join_path` as deprecated


1.0.11 (2022-07-12)
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


1.0.5 (2022-02-06)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- add :meth:`s3pathlib.core.S3Path.us_gov_cloud_console_url` property
- add :func:`s3pathlib.utils.parse_data_size` method

**Minor Improvements**

**Bugfixes**

**Miscellaneous**


1.0.4 (2022-01-25)
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
