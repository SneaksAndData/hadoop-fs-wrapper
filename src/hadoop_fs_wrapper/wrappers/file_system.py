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

"""
 Python Simplification of Hadoop Filesystem
"""

from typing import List, Iterator

from pyspark.sql import SparkSession

from hadoop_fs_wrapper.wrappers.hadoop_fs_wrapper import HadoopFsWrapper
from hadoop_fs_wrapper.models.hadoop_file_status import HadoopFileStatus
from hadoop_fs_wrapper.models.file_status import FileStatus


class FileSystem:
    """
      Wrapper around Hadoop FileSystem (Java)
      https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/fs/FileSystem.html
    """

    def __init__(self, jvm, conf):
        self._jvm = jvm
        self._conf = conf
        self._fsw = HadoopFsWrapper(jvm=self._jvm, conf=self._conf)

    @classmethod
    def from_spark_session(cls, spark: SparkSession):
        """
         Initializes filesystem from a Spark session
        :param spark: SparkSession
        """
        return cls(spark._jvm, spark._jsc.hadoopConfiguration())

    def list_objects(self, path: str, globfilter: str = None) -> List[HadoopFileStatus]:
        """
         Filter files/directories in the given path using the user-supplied path filter.
         Does not guarantee to return the List of files/directories status in a sorted order.
        :param path: a path name
        :param globfilter: the user-supplied path globfilter
        :return: List of type HadoopFileStatus
        """
        fs_path = self._fsw.path(path)
        files = self._fsw.list_status(fs_path) if not globfilter \
            else self._fsw.list_status_filter(fs_path, self._fsw.glob_filter(globfilter))

        if files:
            return list(map(HadoopFileStatus.from_file_status, files))

        return []

    def list_objects_stream(self, *, path: str) -> Iterator[HadoopFileStatus]:
        """
         Returns a remote iterator so that followup calls are made on demand while consuming the entries.
         Each Hadoop FileSystem implementation should override this method and provide a more efficient implementation, if possible.
         Does not guarantee to return the iterator that traverses statuses of the files in a sorted order.

         https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/fs/FileSystem.html#listStatusIterator-org.apache.hadoop.fs.Path-

         :param path: Path to list objects in.
         :return: Iterator of HadoopFileStatus objects
        """
        fs_path = self._fsw.path(path)
        fs_iterator = self._fsw.list_status_iterator(path=fs_path)
        for fs_obj in fs_iterator:
            yield HadoopFileStatus.from_file_status(FileStatus(fs_obj))

    def glob_status(self, path_pattern: str, globfilter: str = None) -> List[HadoopFileStatus]:
        """
         Return an array of FileStatus objects whose path names match path_pattern
         and is accepted by the user-supplied path filter.
         Results are sorted by their path names.
        :param path_pattern: a glob specifying the path pattern
        :param globfilter: a user-supplied path filter
        :return: List of type HadoopFileStatus
        """
        fs_path = self._fsw.path(path_pattern)
        files = self._fsw.glob_status(fs_path) if not globfilter \
            else self._fsw.glob_status_filter(fs_path, self._fsw.glob_filter(globfilter))

        if files:
            return list(map(HadoopFileStatus.from_file_status, files))

        return []

    def delete(self, path: str, recursive: bool = False) -> bool:
        """
         Delete a file.
        :param path: the path to delete
        :param recursive: if path is a directory and set to true,
                          the directory is deleted else throws an exception.
                          In case of a file the recursive can be set to either true or false.
        :return: True if delete is successful else False.
        """
        return self._fsw.delete(self._fsw.path(path), recursive=recursive)

    def exists(self, path: str) -> bool:
        """
         Check if a path exists.
        :param path: source path
        :return: True if the path exists
        """
        return self._fsw.exists(self._fsw.path(path))

    def rename(self, src: str, dst: str) -> bool:
        """
         Renames Path src to Path dst.
         Note: if dst exists this method returns False and file remains
         un-renamed
        :param src: path to be renamed
        :param dst: new path after rename
        :return: True if rename is successful
        """
        return self._fsw.rename(self._fsw.path(src), self._fsw.path(dst))

    def write(self, path: str, data: str, overwrite=False, encoding='utf-8'):
        """
         Writes stringdata to a new file with optional overwrite
        :param path: path to write to
        :param data: stringdata to write
        :param overwrite: if a file with this name already exists,
                          then if true, the file will be overwritten,
                          and if false an exception will be thrown.
        :param encoding: encoding to encode the stringdata
        :return:
        """
        stream = self._fsw.create(self._fsw.path(path), overwrite)
        buffered_stream = self._fsw.buffered_output_stream(stream)
        buffered_stream.write(data.encode(encoding))
        buffered_stream.close()

    def read(self, path: str, encoding='utf-8') -> str:
        """
         reads stringdata from file
        :param path: path to read
        :param encoding: encoding to read
        :return: string
        """
        stream = self._fsw.open(self._fsw.path(path))
        buffered_stream = self._fsw.buffered_input_stream(stream)
        input_stream_reader = self._fsw.input_stream_reader(
            buffered_stream, encoding)
        buffered_reader = self._fsw.buffered_reader(input_stream_reader)
        lines_collector = self._jvm.java.util.stream.Collectors.joining('\n')
        return buffered_reader.underlying.lines().collect(lines_collector)
