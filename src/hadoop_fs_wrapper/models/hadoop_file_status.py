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
Interface that represents the client side information for a file.
"""

from datetime import datetime
from dataclasses import dataclass

from hadoop_fs_wrapper.models.file_status import FileStatus


@dataclass
class HadoopFileStatus:
    """
      Interface that represents the client side information for a file.
    """
    path: str
    owner: str
    group: str
    permission: str
    modified_on: datetime
    type: str
    block_size: int
    length: int

    @classmethod
    def from_file_status(cls, hadoop_file_status: FileStatus):
        """
          Parses the org.apache.hadoop.fs.FileStatus.

        :param hadoop_file_status: source org.apache.hadoop.fs.FileStatus
        :return: object of type HadoopFileStatus
        """
        return cls(
            path=hadoop_file_status.get_path(),
            owner=hadoop_file_status.get_owner(),
            group=hadoop_file_status.get_group(),
            permission=hadoop_file_status.get_permission(),
            modified_on=datetime.utcfromtimestamp(
                hadoop_file_status.get_modification_time() // 1000
            ).replace(
                microsecond=hadoop_file_status.get_modification_time() % 1000 * 1000
            ),
            type="File" if hadoop_file_status.is_file() else "Folder",
            block_size=hadoop_file_status.get_block_size(),
            length=hadoop_file_status.get_file_length()
        )
