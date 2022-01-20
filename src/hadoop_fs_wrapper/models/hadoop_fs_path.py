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
  Wrapper for org.apache.hadoop.fs.Path
"""


class HadoopFsPath:
    """
      Wrapper for org.apache.hadoop.fs.Path
    """
    def __init__(self, underlying):
        """
         Class init
        """
        self.underlying = underlying

    def uri(self):
        """
         Returns java.net.Uri corresponding to the supplied path
        """
        return self.underlying.toUri()

    @classmethod
    def from_string(cls, jvm, path_string: str):
        """
          Creates a new class instance directly from a path string
        """
        return cls(jvm.org.apache.hadoop.fs.Path(path_string))
