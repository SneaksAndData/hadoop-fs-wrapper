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
 Wrapper around Hadoop FileSystem (Java)
"""
from typing import List

from hadoop_fs_wrapper.models.file_status import FileStatus
from hadoop_fs_wrapper.models.remote_iterator import RemoteIterator
from hadoop_fs_wrapper.models.fs_data_input_stream import FSDataInputStream
from hadoop_fs_wrapper.models.fs_data_output_stream import FSDataOutputStream
from hadoop_fs_wrapper.models.hadoop_fs_path import HadoopFsPath
from hadoop_fs_wrapper.models.uri import URI
from hadoop_fs_wrapper.models.buffered_reader import BufferedReader
from hadoop_fs_wrapper.models.buffered_output_stream import BufferedOutputStream
from hadoop_fs_wrapper.models.buffered_input_stream import BufferedInputStream
from hadoop_fs_wrapper.models.glob_filter import GlobFilter
from hadoop_fs_wrapper.models.input_stream_reader import InputStreamReader


class HadoopFsWrapper:
    """
      Wrapper around Hadoop FileSystem (Java)
      https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/fs/FileSystem.html
    """

    def __init__(self, jvm, conf):
        self._jvm = jvm
        self._conf = conf

    def delete(self, path: HadoopFsPath, recursive: bool = True) -> bool:
        """
         Wraps delete(Path f, boolean recursive)
        :param path: the path to delete (Path class from JVM)
        :param recursive: if path is a directory and set to true,
                          the directory is deleted else throws an exception.
                          In case of a file the recursive can be set to either true or false.
        :return: true if delete is successful else false.
        """
        file_system = self.get_file_system(path.uri(), self._conf)
        return file_system.delete(path.underlying, recursive)

    def list_status_filter(self, path: HadoopFsPath, glob_filter: GlobFilter) -> List[FileStatus]:
        """
         Wraps listStatus:
         Filter files/directories in the given path using the user-supplied path filter.
         Does not guarantee to return the List of files/directories status in a sorted order.
        :param path: a path name
        :param glob_filter: the user-supplied path filter
        :return: list of objects of type org.apache.hadoop.fs.FileStatus
        """
        file_system = self.get_file_system(path.uri(), self._conf)
        fss = file_system.listStatus(path.underlying, glob_filter.underlying)

        if fss:
            return list(map(FileStatus, fss))

        return []

    def list_status(self, path: HadoopFsPath) -> List[FileStatus]:
        """
         Wraps listStatus(Path f):
         List the statuses of the files/directories in the given path if the path is a directory.
        :param path: a path name
        :return: list of objects of type org.apache.hadoop.fs.FileStatus
        """
        file_system = self.get_file_system(path.uri(), self._conf)
        fss = file_system.listStatus(path.underlying)
        if fss:
            return list(map(FileStatus, fss))

        return []

    def glob_status(self, path_pattern: HadoopFsPath) -> List[FileStatus]:
        """
         Wraps globStatus(Path f):
         Return all the files that match filePattern and are not checksum files.
         Results are sorted by their names.
        :param path_pattern: a path name
        :return: list of objects of type org.apache.hadoop.fs.FileStatus
        """
        file_system = self.get_file_system(path_pattern.uri(), self._conf)
        globs = file_system.globStatus(path_pattern.underlying)
        if globs:
            return list(map(FileStatus, globs))

        return []

    def glob_status_filter(self, path_pattern: HadoopFsPath, glob_filter: GlobFilter) -> List[FileStatus]:
        """
         Wraps globStatus(Path f):
         Return all the files that match filePattern and are not checksum files.
         Results are sorted by their names.
        :param path_pattern: a path name
        :param glob_filter: glob filter to apply when listing fs.
        :return: list of objects of type org.apache.hadoop.fs.FileStatus
        """
        file_system = self.get_file_system(path_pattern.uri(), self._conf)
        globs = file_system.globStatus(path_pattern.underlying, glob_filter.underlying)
        if globs:
            return list(map(FileStatus, globs))

        return []

    def get_file_system(self, path, conf):
        """
         Wraps get(URI uri, Configuration conf):
         Get a FileSystem for this URI's scheme and authority.
        :param path: a path name
        :param conf: filesystem configuration
        :return: org.apache.hadoop.fs.FileSystem
        """
        return self._jvm.org.apache.hadoop.fs.FileSystem.get(path, conf)

    def exists(self, path: HadoopFsPath) -> bool:
        """
         Wraps exists(Path f):
         Check if a path exists.
        :param path: source path
        :return: true if the path exists
        """
        file_system = self.get_file_system(path.uri(), self._conf)
        return file_system.exists(path.underlying)

    def rename(self, src: HadoopFsPath, dst: HadoopFsPath) -> bool:
        """
         Wraps rename(Path src, Path dst):
         Renames Path src to Path dst.
        :param src: path to be renamed
        :param dst: new path after rename
        :return: true if rename is successful
        """
        file_system = self.get_file_system(src.uri(), self._conf)
        return file_system.rename(src.underlying, dst.underlying)

    def create(self, path: HadoopFsPath, overwrite: bool) -> FSDataOutputStream:
        """
         Wraps create(Path f, Boolean overwrite):
         Create an FSDataOutputStream at the indicated Path.
         Files are overwritten by default.
        :param path: the file to create
        :param overwrite: if a file with this name already exists,
                          then if true, the file will be overwritten,
                          and if false an exception will be thrown.
        :return: FSDataOutputStream
        """
        file_system = self.get_file_system(path.uri(), self._conf)
        return FSDataOutputStream(file_system.create(path.underlying, overwrite))

    def open(self, path: HadoopFsPath) -> FSDataInputStream:
        """
         Wraps open(Path f):
         Opens an FSDataInputStream at the indicated Path.
        :param path: the file to open
        :return: FSDataInputStream
        """
        file_system = self.get_file_system(path.uri(), self._conf)
        return FSDataInputStream(file_system.open(path.underlying))

    # --- Misc Non FileSystem wrappers: ---

    # org.apache.hadoop.fs.Path:
    # Names a file or directory in a FileSystem. Path strings use slash as the directory separator.
    def path(self, path_string: str) -> HadoopFsPath:
        """
         Wraps constructor org.apache.hadoop.fs.Path(String pathString)
         Construct a path from a String.
        :pathString f: the path string
        :return: Path
        """
        return HadoopFsPath.from_string(self._jvm, path_string)

    # java.net.URI:
    # Represents a Uniform Resource Identifier (URI) reference.
    def uri(self, uri_str: str) -> URI:
        """
         Wraps constructor java.net.URI(String str)
         Constructs a URI by parsing the given string.
        :param uri_str: The string to be parsed into a URI
        :return:
        """
        return URI.from_string(self._jvm, uri_str)

    # java.io.BufferedOutputStream
    # The class implements a buffered output stream.
    # By setting up such an output stream,
    # an application can write bytes to the underlying output stream without
    # necessarily causing a call to the underlying system for each byte written.
    def buffered_output_stream(self, output_stream: FSDataOutputStream) -> BufferedOutputStream:
        """
         Wraps constructor java.io.BufferedOutputStream(OutputStream out)
         Creates a new buffered output stream to write data to the specified
         underlying output stream.
        :param output_stream: the underlying output stream.
        :return: BufferedOutputStream
        """
        return BufferedOutputStream.from_output_stream(self._jvm, output_stream.underlying)

    def glob_filter(self, file_pattern: str) -> GlobFilter:
        """
         Wraps GlobFilter(String filePattern):
         Creates a glob filter with the specified file pattern.
        :param file_pattern: the file pattern
        :return:
        """
        return GlobFilter.from_string(self._jvm, file_pattern)

    def buffered_input_stream(self, input_stream: FSDataInputStream) -> BufferedInputStream:
        """
         Wraps constructor java.io.BufferedInputStream(InputStream in)
         Creates a BufferedInputStream and saves its argument,
         the input stream in, for later use.
         An internal buffer array is created and stored in buf.
        :param input_stream: the underlying input stream.
        :return: BufferedOutputStream
        """
        return BufferedInputStream.from_input_stream(self._jvm, input_stream.underlying)

    def buffered_reader(self, input_reader: InputStreamReader) -> BufferedReader:
        """
         Wraps constructor java.io.BufferedReader(Reader in)
         Creates a buffering character-input stream that
         uses a default-sized input buffer.
        :param input_reader: A Reader
        :return: BufferedReader
        """
        return BufferedReader.from_reader(self._jvm, input_reader.underlying)

    def input_stream_reader(self,
                            input_stream: InputStreamReader,
                            charset_name: str = None) -> InputStreamReader:
        """
          Wraps constructor java.io.InputStreamReader(InputStream in)
          Creates an InputStreamReader that uses the named charset.

        :param input_stream: An InputStream
        :param charset_name: The name of a supported charset
        :return: InputStreamReader
        """
        if not charset_name:
            return InputStreamReader.from_input_stream(self._jvm, input_stream.underlying)
        return InputStreamReader.from_input_stream_and_charset(self._jvm,
                                                               input_stream.underlying,
                                                               charset_name)

    def list_status_iterator(self, path: HadoopFsPath) -> RemoteIterator[FileStatus]:
        """
          Wraps public org.apache.hadoop.fs.RemoteIterator<FileStatus> listStatusIterator(Path p)
             throws FileNotFoundException,
             IOException

        :param path: Hadoop FileSystem path.
        :return: Iterator of FileStatus
        """
        file_system = self.get_file_system(path.uri(), self._conf)
        iterator = file_system.listStatusIterator(path.underlying)

        return RemoteIterator[FileStatus](underlying=iterator)
