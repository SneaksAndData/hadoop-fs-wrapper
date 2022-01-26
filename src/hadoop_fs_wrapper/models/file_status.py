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
 Wrapper for org.apache.hadoop.fs.FileStatus
"""


class FileStatus:
    """
     Wrapper for org.apache.hadoop.fs.FileStatus
    """
    def __init__(self, underlying):
        """
         Class init
        """
        self.underlying = underlying

    def get_path(self) -> str:
        """
         Wraps getPath() method.

        """
        return self.underlying.getPath().toString()

    def get_owner(self) -> str:
        """
         Wraps getOwner() method.

        """
        return self.underlying.getOwner()

    def get_group(self) -> str:
        """
         Wraps getGroup() method.

        """
        return self.underlying.getGroup()

    def get_permission(self) -> str:
        """
         Wraps getPermission() method.

        """
        return self.underlying.getPermission().toString()

    def get_modification_time(self) -> int:
        """
         Wraps getModificationTime() method.

        """
        return self.underlying.getModificationTime()

    def is_file(self) -> bool:
        """
         Wraps isFile() method.

        """
        return self.underlying.isFile()

    def get_block_size(self) -> int:
        """
         Wraps getBlockSize() method.

        """
        return self.underlying.getBlockSize()

    def get_file_length(self) -> int:
        """
         Wraps getLen() method.

        """
        return self.underlying.getLen()
