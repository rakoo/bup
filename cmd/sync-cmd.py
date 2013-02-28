#!/usr/bin/env python
import re, struct, errno, time, zlib, sys, time
from bup import git, ssh, options
from bup.helpers import *

from twisted.internet.protocol import Protocol, ClientFactory, Factory
from twisted.protocols.basic import Int32StringReceiver
from twisted.internet import reactor

from collections import deque

MAX_FRAME_SIZE = 40000


class ContentServerProtocol(Int32StringReceiver):

    def __init__(self, repo, push, pull):
        self.MAX_LENGTH = 300000 # some chunks are very large

        self.total_size_sent = 0
        self.total_size_received = 0
        self.beginning = time.time()
        self.cp = git.CatPipe()
        self.total_received = 0

        # A boolean indicating if a content server should push requested
        # objects
        self.push = push

        # A boolean indicating if a content server should pull missing
        # objects
        self.pull = pull

        self.w = git.PackWriter()

        # All the new hashes missing on our side. Used for clean
        # verification at the end.
        self.new_hashes = set()

        # When the transfer is done, the other side might still be
        # sending some data while we verify on our side. This boolean
        # will skip any processing on our side.
        self.transfer_done = False

        git.check_repo_or_die(repo)
        self.packList = git.PackIdxList(os.path.join(repo, "objects/pack"))
        self.packList.refresh()

        self.new_commits = []


    def connectionMade(self):
        if self.push:
            # Upon receiving a connection, send the refs

            log("Received a connection, starting push\n")
            allrefs = []
            for (refname, sha) in git.list_refs():
                allrefs.append(refname + ' ' + sha)
            if len(allrefs) > 1:
                message = 'REFS\n' + '\n'.join(allrefs)
                message = struct.pack("!I", len(message)) + message + '\0'
                self.sendString(message)
            else:
                log("No refs to send !\n")
        else:
            log("Received a connection, not pushing\n")

    def stringReceived(self, data):
        if self.transfer_done:
            return
        local_missing, remote_missing = self._process_data(data)
        for next_messages in self._prepare_next_messages(local_missing,
                                                        remote_missing):
            if len(next_messages) > 0:
                tosend = ''.join(next_messages)
                self.sendString(tosend)

    def _process_data(self, data):
        local_missing = remote_missing = deque()
        for cmd, message in self._decode(data):
            if cmd == 'REFS':
                if not self.pull:
                    continue

                missing = self._treat_refs(message)
                print "missing %s commits" % len(missing)

                if len(missing) == 0:
                    self._end(_decode_refs(message))
                else:
                    self.new_commits.extend(missing)
                    self.new_hashes.update(missing)
                    local_missing.extend(missing)

            elif cmd == 'HAVE':
                self.total_received += 1
                if not self.pull:
                    continue

                hash, type, content = self._decode_have(message)

                assert type in ('blob', 'tree', 'commit')
                assert(self.w.maybe_write(type, content))

                if type == 'commit':
                    # firstline of the commit message contains the tree
                    tree_sha = content.split('\n')[0].split(' ')[1].decode('hex')
                    if _want_object_or_not(tree_sha):
                        self.new_hashes.add(tree_sha)
                        local_missing.append(tree_sha)
                elif type == 'tree':
                    # each line contains an object
                    for (mode, name, sub_hash) in git.tree_decode(content):
                        if _want_object_or_not(sub_hash):
                            self.new_hashes.add(sub_hash)
                            local_missing.append(sub_hash)

                elif type == 'blob':
                    pass
                    #print "blob"

            elif cmd == 'WANT':
                if self.push:
                    remote_missing.append(message)

        return local_missing, remote_missing

    def _want_object_or_not(self, hash):
        """Tell if an object is needed or not. Checks in the repo, the
        hashes not yet received and the hashes received in the
        exchange
        """
        if self.w.exists(hash) or hash in self.new_hashes:
            return False
        else:
            return True

    def _prepare_next_messages(self, local_missing, remote_missing):

        if len(self.new_hashes) == 0: # transfer is over
            #log("transfer should be over, sending a REFS to verify...\n")
            allrefs = []
            for (refname, sha) in git.list_refs():
                allrefs.append(refname + ' ' + sha)
            message = 'REFS\n' + '\n'.join(allrefs)
            message = struct.pack("!I", len(message)) + message + '\0'

            yield [message]

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
                file = self.cp.get(maybe_next_sha.encode('hex'))
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

            self.total_size_sent += total_size
            duration = time.time() - self.beginning
            speed = self.total_size_sent / (duration) / 1024
            qprogress("%s kbps\r" % str(speed))


            yield next_messages

    def _treat_refs(self, data):

        missing = []
        for (name, sha) in _decode_refs(data):
            if _want_object_or_not(sha):
                missing.append(sha)
        return missing

    def _decode_refs(self, refs_message):
        allrefs = []
        for line in data.split('\n'):
            allrefs.append(line.split(' ')) # (name, sha)

        return allrefs

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
        self.total_size_received += len(buf)
        start = 0
        while start < len(buf):
            length = struct.unpack("!I", buf[start:start+4])[0]
            cmd = buf[start+4:start+8]
            end = start + 4 + length
            message = buf[start+9:end]

            assert(buf[end:end+1] == '\0')

            yield cmd, message

            start = end+1

    def _end(self, allrefs):
        tmp_set = set(self.new_hashes)
        for sha in self.w.idx:
            tmp_set.remove(sha)
        assert len(tmp_set) == 0
        self.new_hashes.clear
        self.transfer_done = True
        log("writing tmp pack to repo...\n")
        self.w.close(run_midx=False)
        log("wrote to repo, have a nice day!\n")

class ContentServerFactory(ClientFactory):

    def __init__(self, bup_repo, push=False, pull=False):
        self.push = push
        self.pull = pull
        self.bup_repo = bup_repo

    def buildProtocol(self, addr):
        p = ContentServerProtocol(self.bup_repo, self.push, self.pull)
        p.factory = self
        return p

    def clientConnectionFailed(self, connector, reason):
        print 'connection failed:', reason.getErrorMessage()
        reactor.stop()

    def clientConnectionLost(self, connector, reason):
        print 'connection lost:', reason.getErrorMessage()

def main():

    optspec = """
bup sync [--remote_host host] [--remote_port remote_port] --port port --repo repo
--
push    push commits that are not available remotely
pull    pull commits that are not available locally
remote_host=    hostname to connect to
remote_port=    port to connect to
repo=   repo to use locally
port=   port to listen to
"""
    o = options.Options(optspec)
    (opt, flags, extra) = o.parse(sys.argv[1:])

    if not (opt.repo or opt.port):
        o.fatal("You must give a repo and a local port")

    serverFactory = ContentServerFactory(opt.repo, opt.push, opt.pull)
    reactor.listenTCP(opt.port, serverFactory)

    if opt.remote_host and opt.remote_port:
        reactor.connectTCP(opt.remote_host, opt.remote_port, serverFactory)

    log("Starting reactor...\n")
    reactor.run()

if __name__ == '__main__':
    main()
