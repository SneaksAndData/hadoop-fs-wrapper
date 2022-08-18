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
     Wrapper for org.apache.hadoop.fs.RemoteIterator<T>
"""
from typing import Generic, TypeVar, Iterable, Iterator

T = TypeVar('T')  # pylint: disable=C0103


class RemoteIterator(Generic[T], Iterable[T]):
    """
     Wrapper for org.apache.hadoop.fs.RemoteIterator<T>
    """

    def __iter__(self) -> Iterator[T]:
        return self

    def __init__(self, underlying):
        """
         Class init
        """
        self.underlying = underlying

    def _next(self) -> T:
        """
         Wraps E next()
               throws IOException
               Returns the next element in the iteration.
        """

        return self.underlying.next()

    def _has_next(self) -> bool:
        """
          Wraps boolean hasNext()
                throws IOException
                Returns true if the iteration has more elements.
        """

        return self.underlying.hasNext()

    def __next__(self) -> T:
        if self._has_next():
            return self._next()

        raise StopIteration
