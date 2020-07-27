"""
The MIT License (MIT)

Copyright (c) 2012, Florian Finkernagel <finkernagel@imt.uni-marburg.de>

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""


class PyPipeGraphError(ValueError):
    """Base class for all PyPipelineGraph exceptions"""

    pass


class CycleError(PyPipeGraphError):
    """You created a cycle in your pipegraph,
    this is not supported"""

    pass


class RuntimeError(PyPipeGraphError):
    """A job died for whatever reason. All unaffected jobs will have been done."""

    def __init__(self, value, exceptions):
        super().__init__(value)
        self.exceptions = exceptions

    pass


class RuntimeException(PyPipeGraphError):
    """Something went wrong with the pipegraph, a bug
    (A poorly named exception in light of RuntimeError)
    """


class JobContractError(PyPipeGraphError):
    """One of the jobs did not confirm to it's supposed behaviour"""

    pass


class JobDiedException(PyPipeGraphError):
    """A job went away without signing off"""

    def __init__(self, exit_code):
        self.exit_code = exit_code
        msg = "Exitcode was %s" % self.exit_code
        PyPipeGraphError.__init__(self, msg)


class CommunicationFailure(PyPipeGraphError):
    """something went wrong talking to a worker"""

    pass


class NothingChanged(Exception):
    """For Invariant communication where
    the invariant value changed, but we don't need to invalidate
    the jobs because of it (and we also want the stored value to be updated).

    This is necessary for the FileChecksumInvariant, the filetime might change,
    then we need to check the checksum. If the checksum matches, we need
    a way to tell the Pipegraph to store the new (filetime, filesize, checksum)
    tuple, without invalidating the jobs.
    """

    def __init__(self, new_value):
        self.new_value = new_value
