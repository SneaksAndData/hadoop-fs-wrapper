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


from datetime import datetime
from hadoop_fs_wrapper.models.hadoop_file_status import HadoopFileStatus
from hadoop_fs_wrapper.models.file_status import FileStatus


class MockPath:
    def __init__(self, path):
        self.path = path

    def toString(self):
        return self.path


class MockPermission:
    def __init__(self, permission):
        self.permission = permission

    def toString(self):
        return self.permission


class MockFileStatus:
    def __init__(self, path, owner, group, permission, modification_time, is_file, block_size, length):
        self.path = MockPath(path)
        self.owner = owner
        self.group = group
        self.permission = MockPermission(permission)
        self.modification_time = modification_time
        self.is_file = is_file
        self.block_size = block_size
        self.length = length

    def getPath(self):
        return self.path

    def getOwner(self):
        return self.owner

    def getGroup(self):
        return self.group

    def getPermission(self):
        return self.permission

    def getModificationTime(self):
        return self.modification_time

    def isFile(self):
        return self.is_file

    def getBlockSize(self):
        return self.block_size

    def getLen(self):
        return self.length


def test_parse_hadoop_file_status():
    test_obj = MockFileStatus(path='test.json',
                              owner='owner-name',
                              group='group-name',
                              permission='rwx------',
                              modification_time=1546300800 * 1000,
                              is_file=True,
                              block_size=12345,
                              length=123)
    test = HadoopFileStatus.from_file_status(FileStatus(underlying=test_obj))

    expected = HadoopFileStatus(path='test.json',
                                owner='owner-name',
                                group='group-name',
                                permission='rwx------',
                                modified_on=datetime(2019, 1, 1, 0, 0, 0, 0),
                                type='File',
                                block_size=12345,
                                length=123)

    assert (test == expected)
