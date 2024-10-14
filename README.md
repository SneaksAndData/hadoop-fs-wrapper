# Hadoop FileSystem Java Class Wrapper 
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

Typed Python wrappers for [Hadoop FileSystem](https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/fs/FileSystem.html) class family.

## Installation
You can install this package from `pypi` on any Hadoop or Spark runtime:
```commandline
pip install hadoop-fs-wrapper
```

Select a version that matches hadoop version you are using:

| Hadoop Version / Spark version | Compatible hadoop-fs-wrapper version |
|--------------------------------|:------------------------------------:|
| 3.2.x / 3.2.x                  |                0.4.x                 |
| 3.3.x / 3.3.x                  |             0.4.x, 0.5.x             |
| 3.3.x / 3.4.x                  |                0.6.x                 |
| 3.5.x / 3.5.x                  |                0.7.x                 |

## Usage
Common use case is accessing Hadoop FileSystem from Spark session object:

```python
from hadoop_fs_wrapper.wrappers.file_system import FileSystem

file_system = FileSystem.from_spark_session(spark=spark_session)
```

Then, for example, one can check if there are any files under specified path:
```python
from hadoop_fs_wrapper.wrappers.file_system import FileSystem

def is_valid_source_path(file_system: FileSystem, path: str) -> bool:
    """
     Checks whether a regexp path refers to a valid set of paths
    :param file_system: pyHadooopWrapper FileSystem
    :param path: path e.g. (s3a|abfss|file|...)://hello@world.com/path/part*.csv
    :return: true if path resolves to existing paths, otherwise false
    """
    return len(file_system.glob_status(path)) > 0
```

## Contribution

Currently basic filesystem operations (listing, deleting, search, iterative listing etc.) are supported. If an operation you require is not yet wrapped,
please open an issue or create a PR.

All changes are tested against Spark 3.4 running in local mode.
