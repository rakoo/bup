import re, struct, errno, time, zlib, sys, time
from bup import git, ssh
from bup.helpers import *

from twisted.internet.protocol import Protocol, ClientFactory, Factory
from twisted.protocols.basic import Int32StringReceiver
from twisted.internet import reactor

from collections import deque

MAX_FRAME_SIZE = 40000




class ContentServerProtocol(Int32StringReceiver):

    def __init__(self, bup_repo):
        git.check_repo_or_die(bup_repo)
        self.packList = git.PackIdxList(os.path.join(bup_repo, "objects/pack"))
        self.packList.refresh()

        self.local_missing = deque()
        self.remote_missing = deque()

        self.total_size_exchanged = 0
        self.beginning = time.time()
        self.cp = git.CatPipe()

    def connectionMade(self):
        allrefs = []
        for (refname, sha) in git.list_refs():
            allrefs.append(refname + ' ' + sha)
        message = 'REFS\n' + '\n'.join(allrefs)
        message = struct.pack("!I", len(message)) + message + '\0'
        self.sendString(message)

    def stringReceived(self, data):
        self._process_data(data)
        for next_messages in self._prepare_next_messages():
            if len(next_messages) > 0:
                tosend = ''.join(next_messages)
                self.sendString(tosend)

    def _process_data(self, data):
        for cmd, message in self._decode(data):

            if cmd == 'REFS':
                missing = self._treat_refs(message)
                print "missing %s commits" % len(missing)

                self.local_missing.extend(missing)

            elif cmd == 'HAVE':
                hash, type, content = self._decode_have(message)
                if type == 'commit':
                    # firstline of the commit message contains the tree
                    tree_sha = content.split('\n')[0].split(' ')[1].decode('hex')
                    if not self.packList.exists(tree_sha):
                        self.local_missing.append(tree_sha)
                elif type == 'tree':
                    # each line contains an object
                    for (mode, name, hash) in git.tree_decode(content):
                        if not self.packList.exists(hash):
                            self.local_missing.append(hash)

                elif type == 'blob':
                    pass
                    #print "blob"

                else:
                    print "wrong type:", type

            elif cmd == 'WANT':
                self.remote_missing.append(message)


    def _prepare_next_messages(self):

        while len(self.local_missing) > 0 or len(self.remote_missing) > 0:
            next_messages = []
            total_size = 0

            while len(self.local_missing) > 0:
                maybe_next = self.local_missing[0]
                message = 'WANT %s' % maybe_next
                message = struct.pack("!I", len(message)) + message + '\0'

                if total_size + len(message) < MAX_FRAME_SIZE:
                    self.local_missing.popleft()
                    total_size += len(message)
                    next_messages.append(message)
                else:
                    break

            while len(self.remote_missing) > 0:
                maybe_next_sha = self.remote_missing[0]
                file = self.cp.get(maybe_next_sha.encode('hex'))
                type = file.next()
                content = ''.join(part for part in file)
                message = "HAVE %s %s\n%s" % (maybe_next_sha, type, content)
                message = struct.pack("!I", len(message)) + message + '\0'

                if total_size + len(message) < MAX_FRAME_SIZE or len(next_messages) == 0:
                    self.remote_missing.popleft()
                    next_messages.append(message)
                    total_size += len(message)
                else:
                    break

            self.total_size_exchanged += total_size
            duration = time.time() - self.beginning
            speed = self.total_size_exchanged / (duration) / 1024
            qprogress("%s kbps\r" % str(speed))


            yield next_messages

    def _treat_refs(self, data):
        missing = []
        for line in data.split('\n'):
            (name, sha) = line.split(' ')
            if not self.packList.exists(sha):
                missing.append(sha)
        return missing

    def _decode_have(self, message):
        hash, rem = message[:20], message[21:]
        type = ''
        while type not in ('commit', 'tree', 'blob'):
            type += rem[0]
            rem = rem[1:]
        rem = rem[1:] # discard the '\n'
        return hash, type, rem

    def _decode(self, buf):
        """yield each framed message in the raw datagram received"""
        start = 0
        while start < len(buf):
            length = struct.unpack("!I", buf[start:start+4])[0]
            cmd = buf[start+4:start+8]
            end = start + 4 + length
            message = buf[start+9:end]

            assert(buf[end:end+1] == '\0')

            yield cmd, message

            start = end+1

class ContentServerNotSendingProtocol(ContentServerProtocol):
    def connectionMade(self):
        print "connection"

class ContentServerFactory(ClientFactory):

    def __init__(self, bup_repo):
        self.bup_repo = bup_repo

    def buildProtocol(self, addr):
        p = ContentServerProtocol(self.bup_repo)
        p.factory = self
        return p

    def clientConnectionFailed(self, connector, reason):
        print 'connection failed:', reason.getErrorMessage()
        reactor.stop()

    def clientConnectionLost(self, connector, reason):
        print 'connection lost:', reason.getErrorMessage()

def main():

    bup_repo = sys.argv[1]
    server_port = int(sys.argv[2])
    host = port = None
    if len(sys.argv) == 4:
        host_port = sys.argv[3].split(':')
        host, port = host_port[0], int(host_port[1])

    serverFactory = ContentServerFactory(bup_repo)
    serverFactory.protocol = ContentServerNotSendingProtocol
    reactor.listenTCP(server_port, serverFactory)

    if host and port:
        print "starting client, connecting to %s:%s" % (host, port)
        clientFactory = ContentServerFactory(bup_repo)
        reactor.connectTCP(host, port, clientFactory)

    reactor.run()

if __name__ == '__main__':
    main()
