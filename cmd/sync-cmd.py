import re, struct, errno, time, zlib, sys, time
from bup import git, ssh
from bup.helpers import *

from twisted.internet.protocol import Protocol, ClientFactory, Factory
from twisted.protocols.basic import Int32StringReceiver
from twisted.internet import reactor

from collections import deque

### Protocol Implementation

end="Bye-bye!"
begin="REF"
receive="REC"

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
        if not packList.exists(sha.decode('hex')):
            missing.append(sha.decode('hex'))
    return missing

def want_next_missing():
    if len(local_missing) > 0:
        return 'WANT %s' % local_missing.popleft()
    else:
        return

local_missing = deque()
total_size_exchanged = 0
beginning = time.time()
def process_data(data):
    global local_missing
    global total_size_exchanged
    global beginning
    cmd, message = data[0:4], data[5:]
    if cmd == 'REFS':
        missing = treat_refs(message)
        print "missing %s commits" % len(missing)

        local_missing.extend(missing)
        return want_next_missing()

    elif cmd == 'HAVE':
        splitted = message.split('\n')
        hash, type = splitted[0].split(' ')
        if type == 'commit':
            # firstline of the commit message contains the tree
            tree_sha = splitted[1].split(' ')[1].decode('hex')
            if not packList.exists(tree_sha):
                local_missing.append(tree_sha)
            else:
                print "I already have", '\n'.join(splitted[1:])
        elif type == 'tree':
            # each line contains an object
            for (mode, name, hash) in git.tree_decode('\n'.join(splitted[1:])):
                if not packList.exists(hash):
                    local_missing.append(hash)

        elif type == 'blob':
            total_size_exchanged += len(message)
            duration = time.time() - beginning
            speed = total_size_exchanged / (duration) / 1024
            log("%s kbps\n" % str(speed))
        return want_next_missing()

    elif cmd == 'WANT':
        sha = message.encode('hex')
        file = cp().get(sha)
        type = file.next()
        content = ''.join(part for part in file)

        return "HAVE %s %s\n%s" % (sha, type, content)

class ContentServer(Int32StringReceiver):

    def stringReceived(self, data):
        next_message = process_data(data)
        if next_message:
            self.sendString(next_message)
        else:
            self.transport.loseConnection()

class ContentClient(Int32StringReceiver):

    def connectionMade(self):

        allrefs = []
        for (refname, sha) in git.list_refs():
            allrefs.append(refname + ' ' + sha.encode('hex'))
        self.sendString('REFS\n' + '\n'.join(allrefs))

    def stringReceived(self, data):
        next_message = process_data(data)
        if next_message:
            self.sendString(next_message)
        else:
            self.transport.loseConnection()


class ContentClientFactory(ClientFactory):
    protocol = ContentClient

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
    serverFactory.protocol = ContentServer
    reactor.listenTCP(server_port, serverFactory)

    if client_connects_to:
        print "starting client"
        clientFactory = ContentClientFactory()
        reactor.connectTCP('localhost', client_connects_to, clientFactory)

    reactor.run()

if __name__ == '__main__':
    main()
