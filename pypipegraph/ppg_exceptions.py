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

class PyPipelineGraphError(ValueError):
    """Base class for all PyPipelineGraph exceptions"""
    pass


class CycleError(PyPipelineGraphError):
    """You created a cycle in your pipegraph,
    this is not supported"""
    pass


class RuntimeError(PyPipelineGraphError):
    """A job died"""
    pass


class RuntimeException(PyPipelineGraphError):
    """Something went wrong with the pipegraph, a bug"""


class JobContractError(PyPipelineGraphError):
    """One of the jobs did not confirm to it's supposed behaviour"""
    pass


class JobDiedException(PyPipelineGraphError):
    """A job went away without signing off"""

    def __init__(self, exit_code):
        self.exit_code = exit_code
        msg = "Exitcode was %s" % self.exit_code
        PyPipelineGraphError.__init__(self, msg)


class CommunicationFailure(PyPipelineGraphError):
    """something went wrong talking to a slave"""
    pass
