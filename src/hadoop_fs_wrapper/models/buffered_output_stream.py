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
  Wrapper for java.io.BufferedOutputStream
"""


class BufferedOutputStream:
    """
      Wrapper for java.io.BufferedOutputStream
    """
    def __init__(self, underlying):
        """
         Class init
        """
        self.underlying = underlying

    def write(self, data):
        """
         Wraps write(data) method.
         Writes the specified byte to this buffered output stream.
        :param data: the data.
        :return:
        """
        return self.underlying.write(data)

    def flush(self):
        """
         Wraps flush() method.
         Flushes this buffered output stream.
         This forces any buffered output bytes to be written out to the
         underlying output stream.
        :return:
        """
        return self.underlying.flush()

    def close(self):
        """
         Closes this output stream and releases any system resources associated with the stream.
         The close method of FilterOutputStream calls its flush method,
         and then calls the close method of its underlying output stream.
         underlying output stream.
        :return:
        """
        return self.underlying.close()

    @classmethod
    def from_output_stream(cls, jvm, output_stream):
        """
         Wraps constructor java.io.BufferedOutputStream(OutputStream out)
         Creates a new buffered output stream to write data to the specified
         underlying output stream.
        :param output_stream: the underlying output stream.
        :return: BufferedOutputStream
        """
        return cls(jvm.java.io.BufferedOutputStream(output_stream))
