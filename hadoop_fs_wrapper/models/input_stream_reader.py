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
  Wrapper for java.io.InputStreamReader
"""


class InputStreamReader:
    """
      Wrapper for java.io.InputStreamReader
    """
    def __init__(self, underlying):
        """
         Class init
        """
        self.underlying = underlying

    @classmethod
    def from_input_stream_and_charset(cls, jvm, input_stream, charset_name):
        """
         Wraps constructor java.io.InputStreamReader(InputStream in, String charsetName)
         Creates an InputStreamReader that uses the named charset.
        :param input_stream: An InputStream
        :param charset_name: The name of a supported charset
        :return: InputStreamReader
        """
        return cls(jvm.java.io.InputStreamReader(input_stream,charset_name))

    @classmethod
    def from_input_stream(cls, jvm, input_stream):
        """
         Wraps constructor java.io.InputStreamReader(InputStream in)
         Creates an InputStreamReader that uses the default charset.
        :param input_stream: An InputStream
        :return: InputStreamReader
        """
        return cls(jvm.java.io.InputStreamReader(input_stream))
