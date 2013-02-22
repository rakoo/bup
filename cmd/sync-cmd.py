import re, struct, errno, time, zlib, sys, time
from bup import git, ssh
from bup.helpers import *

from twisted.internet.protocol import Protocol, ClientFactory, Factory
from twisted.protocols.basic import Int32StringReceiver
from twisted.internet import reactor

from collections import deque

MAX_FRAME_SIZE = 1300

_cp = None
def cp():
    """Create a git.CatPipe object or reuse the already existing one."""
    global _cp
    if not _cp:
        _cp = git.CatPipe()
    return _cp

def extract_sha_from_commit(commit):
    treeline = commit.split('\n')[0]
    assert(treeline.startswith('tree '))
    return treeline.split(' ')[1].decode('hex')

def treat_refs(data):
    missing = []
    for line in data.split('\n'):
        (name, sha) = line.split(' ')
        if not packList.exists(sha):
            missing.append(sha)
    return missing

def decode_have(message):
    hash, rem = message[:20], message[21:]
    type = ''
    while type not in ('commit', 'tree', 'blob'):
        type += rem[0]
        rem = rem[1:]
    rem = rem[1:] # discard the '\n'
    return hash, type, rem

total_size_exchanged = 0
beginning = time.time()
blobs_exchanged = 0
def prepare_next_messages():
    global total_size_exchanged
    global beginning
    global blobs_exchanged

    while len(local_missing) > 0 or len(remote_missing) > 0:
        next_messages = []
        total_size = 0

        while len(local_missing) > 0:
            maybe_next = local_missing[0]
            message = 'WANT %s' % maybe_next
            message = struct.pack("!I", len(message)) + message + '\0'

            if total_size + len(message) < MAX_FRAME_SIZE:
                local_missing.popleft()
                total_size += len(message)
                next_messages.append(message)
            else:
                break

        while len(remote_missing) > 0:
            maybe_next_sha = remote_missing[0]
            file = cp().get(maybe_next_sha.encode('hex'))
            type = file.next()
            content = ''.join(part for part in file)
            message = "HAVE %s %s\n%s" % (maybe_next_sha, type, content)
            message = struct.pack("!I", len(message)) + message + '\0'

            if total_size + len(message) < MAX_FRAME_SIZE or len(next_messages) == 0:
                remote_missing.popleft()
                next_messages.append(message)
                total_size += len(message)
            else:
                break

        total_size_exchanged += total_size
        duration = time.time() - beginning
        speed = total_size_exchanged / (duration) / 1024
        qprogress("%s kbps\r" % str(speed))

        #qprogress("%s messages, %s local_missing remaining and %s remote_missing remaining\n" % (len(next_messages),
                                                 #len(local_missing),
                                                 #len(remote_missing)))

        yield next_messages

def decode(buf):
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

local_missing = deque()
remote_missing = deque()

def process_data(data):
    global local_missing

    for cmd, message in decode(data):

        if cmd == 'REFS':
            missing = treat_refs(message)
            print "missing %s commits" % len(missing)

            local_missing.extend(missing)

        elif cmd == 'HAVE':
            hash, type, content = decode_have(message)
            if type == 'commit':
                # firstline of the commit message contains the tree
                tree_sha = content.split('\n')[0].split(' ')[1].decode('hex')
                if not packList.exists(tree_sha):
                    local_missing.append(tree_sha)
            elif type == 'tree':
                # each line contains an object
                for (mode, name, hash) in git.tree_decode(content):
                    if not packList.exists(hash):
                        local_missing.append(hash)

            elif type == 'blob':
                pass
                #print "blob"

            else:
                print "wrong type:", type

        elif cmd == 'WANT':
            remote_missing.append(message)

class ContentServerProtocol(Int32StringReceiver):

    def connectionMade(self):
        allrefs = []
        for (refname, sha) in git.list_refs():
            allrefs.append(refname + ' ' + sha)
        message = 'REFS\n' + '\n'.join(allrefs)
        message = struct.pack("!I", len(message)) + message + '\0'
        self.sendString(message)

    def stringReceived(self, data):
        process_data(data)
        for next_messages in prepare_next_messages():
            if len(next_messages) > 0:
                tosend = ''.join(next_messages)
                self.sendString(tosend)


class ContentServerNotSendingProtocol(ContentServerProtocol):
    def connectionMade(self):
        print "connection"

class ContentClientFactory(ClientFactory):
    protocol = ContentServerProtocol

    def clientConnectionFailed(self, connector, reason):
        print 'connection failed:', reason.getErrorMessage()
        reactor.stop()

    def clientConnectionLost(self, connector, reason):
        print 'connection lost:', reason.getErrorMessage()

def main():

    bup_repo = sys.argv[1]
    server_port = int(sys.argv[2])
    if len(sys.argv) == 4:
        client_connects_to = int(sys.argv[3])
    else:
        client_connects_to = None

    global packList
    packList = git.PackIdxList(os.path.join(bup_repo, "objects/pack"))
    packList.refresh()

    git.check_repo_or_die(bup_repo)
    serverFactory = Factory()
    serverFactory.protocol = ContentServerNotSendingProtocol
    reactor.listenTCP(server_port, serverFactory)

    if client_connects_to:
        print "starting client"
        clientFactory = ContentClientFactory()
        reactor.connectTCP('localhost', client_connects_to, clientFactory)

    reactor.run()

if __name__ == '__main__':
    main()
