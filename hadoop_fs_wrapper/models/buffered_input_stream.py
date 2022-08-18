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
  Wrapper for java.io.BufferedInputStream
"""


class BufferedInputStream:
    """
      Wrapper for java.io.BufferedInputStream
    """
    def __init__(self, underlying):
        """
         Class init
        """
        self.underlying = underlying

    @classmethod
    def from_input_stream(cls, jvm, input_stream):
        """
         Wraps constructor java.io.BufferedInputStream(InputStream in)
         Creates a BufferedInputStream and saves its argument,
         the input stream in, for later use.
         An internal buffer array is created and stored in buf.
        :param input_stream: the underlying input stream.
        :return: BufferedOutputStream
        """
        return cls(jvm.java.io.BufferedInputStream(input_stream))
