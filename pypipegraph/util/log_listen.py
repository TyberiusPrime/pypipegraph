from twisted.internet.protocol import Protocol, Factory
from twisted.internet import reactor
import logging
import cPickle

of = open("log.txt",'wb')

class Debug(Protocol):

    def dataReceived(self, data):
        global of
        try:
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

factory = Factory()
factory.protocol= Debug
reactor.listenTCP(5005, factory)
reactor.run()
