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

By default, Machete don't text", er,  "twisted don't fork"
for fear of weirdish signal interactions.

Now, I boldly assume that that can get handled and introduce forking process 
to twisted, which we need to replicate the multiprocessing solution we have
in LocalResourceCoordinator and in the old pypipeline
"""

from twisted.internet import process
import gc
import traceback
import os
import sys

class ForkedProcess(process.Process):

    def __init__(self, reactor, callback, proto):
        process.Process.__init__(self, reactor, callback, (), {}, False,
                proto, uid=None, gid=None, childFDs = None)


    def _execChild(self, path, settingUID, uid, gid, callback, args, environment):
        if args:
            raise ValueError("args not supported")
        if path:
            raise ValueError("Path not supported")
        if settingUID:
            raise ValueError("settingUID not supported")
        #now, this is in the forked child already, and inside the try: except block of _BaseProcess._fork 
        #make sure we get the pipes right...
        sys.stdout = os.fdopen(1, 'w')
        sys.stderr = os.fdopen(2, 'w')
        sys.stdout.flush()
        sys.stderr.flush()
        gc.enable()
        #so... go for it
        try:
            callback()
        except SystemExit, e:
            sys.stdout.flush()
            sys.stderr.flush()
            os._exit(e.code)
        except:
            # If there are errors, bail and try to write something
            # descriptive to the forked process' stderr.
            try:
                #stderr = os.fdopen(2, 'w')
                stderr = sys.stderr
                stderr.write("Upon callback %s" % callback)
                traceback.print_exc(file=stderr)
                sys.stdout.flush()
                sys.stderr.flush()
                for fd in range(3):
                    os.close(fd)
                os._exit(1)
            except:
                pass # make *sure* the child terminates
                sys.stdout.flush()
                sys.stderr.flush()
                os._exit(1)
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(0)
