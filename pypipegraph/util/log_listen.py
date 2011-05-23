from twisted.internet.protocol import Protocol, Factory
from twisted.internet import reactor
import logging
import cPickle

class Debug(Protocol):

    def dataReceived(self, data):
        lr = logging.makeLogRecord(cPickle.loads(data[4:]))
        print "%s: %s" % (lr.name, lr.getMessage())
        #self.transport.write(data, (host, port))

factory = Factory()
factory.protocol= Debug
reactor.listenTCP(5005, factory)
reactor.run()
