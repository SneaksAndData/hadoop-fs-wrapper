# MIT License
#
# Copyright (c) 2022 SneaksAndData
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import pathlib

import pytest
from pyspark.sql import SparkSession

from hadoop_fs_wrapper.wrappers.file_system import FileSystem


def test_list_objects(spark_session: SparkSession):
    test_data_path = f"{pathlib.Path(__file__).parent.resolve()}/data"

    try:
        fs = FileSystem.from_spark_session(spark_session)
        files = fs.list_objects(f"file:///{test_data_path}/partitionedFolder/")
        assert len(files) == 2, "List operation didn't find all required files"
    except RuntimeError:
        pytest.fail("Failed to run list_objects")


def test_list_objects_stream(spark_session: SparkSession):
    test_data_path = f"{pathlib.Path(__file__).parent.resolve()}/data"

    try:
        fs = FileSystem.from_spark_session(spark_session)
        files = fs.list_objects_stream(path=f"file:///{test_data_path}/partitionedFolder/")
        counter = 0
        for _ in files:
            counter += 1
        assert counter == 2, "List (streaming) operation didn't find all required files"
    except RuntimeError:
        pytest.fail("Failed to run list_objects")


def test_glob_status(spark_session: SparkSession):
    test_data_path = f"{pathlib.Path(__file__).parent.resolve()}/data"

    try:
        fs = FileSystem.from_spark_session(spark_session)
        files = fs.glob_status(f"file:///{test_data_path}/partitionedFolder/partition=*/data.csv")
        assert len(files) == 2, "List operation didn't find all required files"
    except RuntimeError:
        pytest.fail("Failed to run glob_status")


def test_read_file(spark_session: SparkSession):
    test_data_path = f"{pathlib.Path(__file__).parent.resolve()}/data"
    expected_data = '"a";"b"\n1;2'

    try:
        fs = FileSystem.from_spark_session(spark_session)
        data = fs.read(f"file:///{test_data_path}/partitionedFolder/partition=1/data.csv")
        assert data == expected_data, "Read operation didn't return expected output"
    except RuntimeError:
        pytest.fail("Failed to run test_read")


@pytest.mark.parametrize(
    "path, expected",
    [
        pytest.param("partitionedFolder/partition=1/data.csv", True, id="true"),
        pytest.param("partitionedFolder/partition=1/non-existing.csv", False, id="false"),
    ],
)
def test_exists(spark_session: SparkSession, path, expected):
    test_data_path = f"{pathlib.Path(__file__).parent.resolve()}/data"

    try:
        fs = FileSystem.from_spark_session(spark_session)
        result = fs.exists(f"file:///{test_data_path}/{path}")
        assert result == expected, "FileSystem.exists() did not return the expected result"
    except RuntimeError:
        pytest.fail("Failed to run test_exists_true")


def test_write_and_read(spark_session: SparkSession):
    test_data_path = f"{pathlib.Path(__file__).parent.resolve()}/data"
    sample_data = "hello world!"
    sample_data_encoding = "utf-8"
    try:
        fs = FileSystem.from_spark_session(spark_session)
        path = f"file:///{test_data_path}/test/newFolder/test.csv"
        fs.write(path=path, data=sample_data, overwrite=True, encoding=sample_data_encoding)
        result = fs.read(path=path, encoding=sample_data_encoding)
        assert result == sample_data, "The written data does not match the expected"
    except RuntimeError:
        pytest.fail("Failed to run test_write_and_read")


def test_write_delete_exists(spark_session: SparkSession):
    test_data_path = f"{pathlib.Path(__file__).parent.resolve()}/data"
    sample_data = "hello world!"
    sample_data_encoding = "utf-8"
    try:
        fs = FileSystem.from_spark_session(spark_session)
        path = f"file:///{test_data_path}/test/deleteThis/test.csv"

        fs.write(path=path, data=sample_data, overwrite=True, encoding=sample_data_encoding)
        assert fs.exists(path=path), "The written file was not found"

        fs.delete(path=path)
        assert not fs.exists(path=path), "The deleted file still exists"

    except RuntimeError:
        pytest.fail("Failed to run test_write_delete_exists")


def test_write_rename(spark_session: SparkSession):
    test_data_path = f"{pathlib.Path(__file__).parent.resolve()}/data"
    sample_data = "hello world!"
    sample_data_encoding = "utf-8"
    try:
        fs = FileSystem.from_spark_session(spark_session)
        path_initial = f"file:///{test_data_path}/test/renameThis/data.csv"
        path_final = f"file:///{test_data_path}/test/renamed/data.csv"

        fs.write(path=path_initial, data=sample_data, overwrite=True, encoding=sample_data_encoding)
        assert fs.exists(path=path_initial), "The written file was not found"

        fs.delete(path_final)
        fs.rename(src=path_initial, dst=path_final)
        assert not fs.exists(path=path_initial), "The initial file still exists"
        assert fs.exists(path=path_final), "The renamed file doesn't exists"

    except RuntimeError:
        pytest.fail("Failed to run test_write_rename")


@pytest.mark.parametrize(
    "path, expected",
    [
        pytest.param("testFileLength/test_empty", 0, id="empty"),
        pytest.param("testFileLength/test_1_char", 1, id="1 char"),
        pytest.param("testFileLength/test_11_chars", 11, id="11 chars"),
    ],
)
def test_length(spark_session: SparkSession, path, expected):
    test_data_path = f"{pathlib.Path(__file__).parent.resolve()}/data"

    try:
        fs = FileSystem.from_spark_session(spark_session)
        result = fs.list_objects(f"file:///{test_data_path}/{path}")[0].length
        assert result == expected, "FileSystem.list_object().length did not return the expected result"
    except RuntimeError:
        pytest.fail("Failed to run FileSystem.list_object().length")
