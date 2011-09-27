from twisted.internet.protocol import Protocol, Factory, ClientFactory
from twisted.internet.error import CannotListenError
from twisted.internet import reactor
import logging
import cPickle

import sys
try:
    port= int(sys.argv[1])
except IndexError:
    port = 5005

of = open("log.txt",'wb')

class Debug(Protocol):

    def dataReceived(self, data):
        global of
        try:
            if data == "please exit":
                print "End log because other logger requested access"
                of.write("End log because other logger requested access")
                reactor.stop()
            lr = logging.makeLogRecord(cPickle.loads(data[4:]))
            if lr.getMessage().find("New Pipegraph") != -1:
                of.close()
                of = open('log.txt','wb')
            print "%s:%i: %s" % (lr.name, lr.lineno, lr.getMessage())
            of.write("%s:%i: %s\n" % (lr.name, lr.lineno, lr.getMessage()))
            of.flush()
        except cPickle.UnpicklingError:
            print 'a messages was missing'
        #self.transport.write(data, (host, port))

class Killer(Protocol):

    def connectionMade(self):
        print 'sending please exit'
        d = self.transport.write("please exit") # no need for address
        self.transport.loseConnection()

    def connectionLost(self, reason):
        print "Other logger apperantly exited, now trying to listen again in 2 seconds"
        reactor.callLater(2, start_listening)

class KillerFactory(ClientFactory):
    protocol = Killer

def start_listening():
    factory = Factory()
    factory.protocol= Debug
    def listening():
        print 'now listening'
    factory.startFactory = listening
    reactor.listenTCP(port, factory)


try:
    start_listening()
except CannotListenError:
    print 'trying to send kill signal to already running instance'
    d = reactor.connectTCP("localhost", port, KillerFactory())



#print "Going to listen on port %i, logging to %s" % (port, of.name)
reactor.run()
