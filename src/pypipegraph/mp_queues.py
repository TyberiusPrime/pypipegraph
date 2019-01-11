"""Fix Queue for running a pypipegraph within an import 
python <2.7 only - see https://bugs.python.org/issue22853"""
import sys
if sys.version_info[0] == 2 and sys.version_info[1] < 7: # pragma: no cover 
    import threading
    from multiprocessing.util import is_exiting, debug, Finalize
    import multiprocessing.queues

    Queue = multiprocessing.queues.Queue
    import weakref
    import traceback

    _sentinel = multiprocessing.queues._sentinel
    import sys
    import os
    import pickle


    class MPQueueFixed(multiprocessing.queues.Queue):  
        def _start_thread(self):
            debug("Queue._start_thread()")

            # Start thread which transfers data from buffer to pipe
            self._buffer.clear()
            self._thread = threading.Thread(
                target=MPQueueFixed._feed,
                args=(
                    self._buffer,
                    self._notempty,
                    self._send,
                    self._wlock,
                    self._writer.close,
                ),
                name="QueueFeederThread",
            )
            self._thread.daemon = True

            debug("doing self._thread.start()")
            self._thread.start()
            debug("... done self._thread.start()")

            # On process exit we will wait for data to be flushed to pipe.
            if not self._joincancelled:
                self._jointhread = Finalize(
                    self._thread,
                    Queue._finalize_join,
                    [weakref.ref(self._thread)],
                    exitpriority=-5,
                )

            # Send sentinel to the thread queue object when garbage collected
            self._close = Finalize(
                self, Queue._finalize_close, [self._buffer, self._notempty], exitpriority=10
            )

        @staticmethod  # noqa:C901
        def _feed(buffer, notempty, send, writelock, close):
            debug("starting thread to feed data to pipe")

            nacquire = notempty.acquire
            nrelease = notempty.release
            nwait = notempty.wait
            bpopleft = buffer.popleft
            sentinel = _sentinel
            if sys.platform != "win32":
                wacquire = writelock.acquire
                wrelease = writelock.release
            else:
                wacquire = None

            try:
                while 1:
                    nacquire()
                    try:
                        if not buffer:
                            nwait()
                    finally:
                        nrelease()
                    try:
                        while 1:
                            obj = bpopleft()
                            if obj is sentinel:
                                debug("feeder thread got sentinel -- exiting")
                                close()
                                return

                            if wacquire is None:
                                send(obj)
                            else:
                                wacquire()
                                try:
                                    # print ('sending object of size %i"' %
                                    # sys.getsizeof(obj, -1) )
                                    # print str(obj)[:100]
                                    # print ""
                                    # print ""
                                    try:
                                        send(obj)
                                    except SystemError as e:
                                        print("Que sending error %s" % e)
                                        print("error dump in %i.dump" % (os.getpid(),))
                                        print(
                                            "Likely source: stdout/stderr too large (gigabytes)"
                                        )
                                        with open("%i.dump" % (os.getpid(),), "wb") as op:
                                            try:
                                                pickle.dump(obj, op)
                                            except Exception:
                                                pass
                                            try:
                                                op.write("%s" % (obj,)[:20000])
                                            except Exception:
                                                pass

                                        raise
                                finally:
                                    wrelease()
                    except IndexError:
                        pass
            except Exception as e:
                # Since this runs in a daemon thread the resources it uses
                # may be become unusable while the process is cleaning up.
                # We ignore errors which happen after the process has
                # started to cleanup.
                try:
                    if is_exiting():
                        debug("error in queue thread: %s", e)
                    else:
                        traceback.print_exc()
                except Exception:
                    pass
