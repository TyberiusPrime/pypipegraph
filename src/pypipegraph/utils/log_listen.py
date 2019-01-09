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
from twisted.internet.protocol import Protocol, Factory, ClientFactory
from twisted.internet.error import CannotListenError
from twisted.internet import reactor
import logging
import cPickle

import sys

try:
    port = int(sys.argv[1])
except IndexError:
    port = 5005

of = open("log.txt", "wb")


class Debug(Protocol):
    def dataReceived(self, data):
        global of
        try:
            if data == "please exit":
                print("End log because other logger requested access")
                of.write("End log because other logger requested access")
                reactor.stop()
            lr = logging.makeLogRecord(cPickle.loads(data[4:]))
            if lr.getMessage().find("New Pipegraph") != -1:
                of.close()
                of = open("log.txt", "wb")
            print("%s:%i: %s" % (lr.name, lr.lineno, lr.getMessage()))
            of.write("%s:%i: %s\n" % (lr.name, lr.lineno, lr.getMessage()))
            of.flush()
        except cPickle.UnpicklingError:
            print("a messages was missing")
        # self.transport.write(data, (host, port))


class Killer(Protocol):
    def connectionMade(self):
        print("sending please exit")
        self.transport.write("please exit")  # no need for address
        self.transport.loseConnection()

    def connectionLost(self, reason):
        print("Other logger apperantly exited, now trying to listen again in 2 seconds")
        reactor.callLater(2, start_listening)


class KillerFactory(ClientFactory):
    protocol = Killer


def start_listening():
    factory = Factory()
    factory.protocol = Debug

    def listening():
        print("now listening")

    factory.startFactory = listening
    reactor.listenTCP(port, factory)


try:
    start_listening()
except CannotListenError:
    print("trying to send kill signal to already running instance")
    d = reactor.connectTCP("localhost", port, KillerFactory())

# print "Going to listen on port %i, logging to %s" % (port, of.name)
reactor.run()
