#!/usr/bin/env python
import re, struct, errno, time, zlib, sys, time
from bup import git, ssh, options
from bup.helpers import *

from twisted.internet.protocol import Protocol, ClientFactory, Factory
from twisted.protocols.basic import Int32StringReceiver
from twisted.internet import reactor

from collections import deque

import re

MAX_FRAME_SIZE = 40000
CAPABILITIES = set()
CAPABILITIES.add('PUSH')
CAPABILITIES.add('PULL')

class SimpleHashList:
    """A class to hold a list of shas and answer to the #exists method.
    Used for the new packwriter objcache, because we don't want to mix
    it with the repo one (since it can be interrupted, thus invalidated,
    at any time.
    """

    def __init__(self):
        self.set = set()

    def __iter__(self):
        return iter(self.set)

    def add(self, sha):
        self.set.add(sha)

    def exists(self, sha, want_source=False):
        return sha in self.set

def _get_simple_objcache():
    return SimpleHashList()

class TransferValidator():
    """A class to validate the transfer while it is ongoing, instead of
    waiting for it to end.

    The protocol exchanges information top-down, with a parent being
    totally defined by its children. This class offers a way to validate
    a parent as soon as the last of its children arrives. The end of the
    transfer happens when the root is validated.
    """

    def __init__(self):

        # A dict with the parents as keys, and the children as values.
        # Filled when a new chunk is claimed, unfilled when this chunk
        # is received.
        # When the transfer is over, it should be empty.
        self.parent_to_children = {}

        # A dict with a children as a key and its parent as a value.
        # Filled when a new chunk is claimed, unfilled when this chunk
        # is received.
        # When the transfer is over, it should be empty.
        self.children_to_parent = {}

        self.commits = set()

        self.validated = True

    def commits_claimed(self, commit_hashes):
        for commit in commit_hashes:
            self.validated = False
            self.commits.add(commit)

    def tree_claimed(self, commit_hash, tree_hash):
        self.commits.add(commit_hash)
        self._object_claimed(commit_hash, tree_hash)

    def tree_object_claimed(self, tree_hash, object_hash):
        self._object_claimed(tree_hash, object_hash)

    def _object_claimed(self, parent_hash, children_hash):
        self.validated = False

        if not parent_hash in self.parent_to_children:
            self.parent_to_children[parent_hash] = set()
        self.parent_to_children[parent_hash].add(children_hash)

        self.children_to_parent[children_hash] = parent_hash

    def hash_received(self, hash):

        # First verify that there are no children anymore
        if hash in self.parent_to_children:
            if len(self.parent_to_children[hash]) > 0:
                return
            else:
                del self.parent_to_children[hash]

        # Then take care of its parent
        if hash in self.children_to_parent:
            parent = self.children_to_parent[hash]
            self.parent_to_children[parent].remove(hash)
            del self.children_to_parent[hash]

            # try to validate the parent if possible
            self.hash_received(parent)

        elif hash in self.commits:
            self.commits.remove(hash)
            if len(self.commits) == 0:
                self.validated = True

    def is_over(self):
        return self.validated

    def is_claimed(self, hash):
        return hash in self.children_to_parent

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

        self.w = git.PackWriter(objcache_maker=_get_simple_objcache)

        # When the transfer is done, the other side might still be
        # sending some data while we verify on our side. This boolean
        # will skip any processing on our side.
        self.transfer_done = False

        git.check_repo_or_die(repo)
        self.packList = git.PackIdxList(os.path.join(repo, "objects/pack"))
        self.packList.refresh()

        self.new_commits = []

        self.validator = TransferValidator()

    def __del__(self):
        """close the object in charge of the communication with the
        client. We have to make sure the pack isn't written in the
        repo.

        This method should never be called manually, only when the line
        is closed (ie after the pack is cleanly appended to the repo) or
        when there is an error.
        """
        self.w.abort()


    def connectionMade(self):
        log("Received a connection, sending capabilities\n")
        capabilities = set()
        if self.push:
            capabilities.add('PUSH')
        if self.pull:
            capabilities.add('PULL')

        message = 'CAPA\n' + '\n'.join(capabilities)
        message = struct.pack("!I", len(message)) + message + '\0'
        self.sendString(message)

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
        local_missing = deque()
        remote_missing = deque()
        for cmd, message in self._decode(data):
            if cmd == 'CAPA':
                stop_ops = False

                remote_capabilities = self._decode_capabilities(message)
                if self.push and not 'PULL' in remote_capabilities:
                    log("trying to push on a remote that doesn't pull.\n")
                    stop_ops = True
                if self.pull and not 'PUSH' in remote_capabilities:
                    log("trying to pull on a remote that doesn't push.\n")
                    stop_ops = True

                if not stop_ops:
                    allrefs = []
                    for (refname, sha) in git.list_refs():
                        allrefs.append(refname + ' ' + sha)
                    if len(allrefs) > 0:
                        message = 'REFS\n' + '\n'.join(allrefs)
                        message = struct.pack("!I", len(message)) + message + '\0'
                        self.sendString(message)

            elif cmd == 'REFS':

                missing_here, missing_there = self._treat_refs(message)

                if self.pull:
                    if self.validator.is_over():
                        self._end(self._decode_refs(message))
                    else:
                        self.new_commits.extend(missing_here)
                        self.validator.commits_claimed(missing_here)
                        local_missing.extend(missing_here)

                if self.push:
                    if len(missing_there) == 0:
                        self.transport.loseConnection()

            elif cmd == 'HAVE':
                self.total_received += 1
                if not self.pull:
                    continue

                hash, type, content = self._decode_have(message)

                assert type in ('blob', 'tree', 'commit')
                self._write_chunk(type, hash, content)

                if type == 'commit':
                    # firstline of the commit message contains the tree
                    tree_sha = content.split('\n')[0].split(' ')[1].decode('hex')

                    if self._want_object_or_not(tree_sha):
                        self.validator.tree_claimed(hash, tree_sha)
                        local_missing.append(tree_sha)

                elif type == 'tree':
                    # each line contains an object
                    for (mode, name, sub_hash) in git.tree_decode(content):
                        if self._want_object_or_not(sub_hash):
                            self.validator.tree_object_claimed(hash, sub_hash)
                            local_missing.append(sub_hash)

                elif type == 'blob':
                    self.validator.hash_received(hash)
                    #print "blob"

            elif cmd == 'WANT':
                if self.push:
                    remote_missing.append(message)

        return local_missing, remote_missing

    def _decode_capabilities(self, message):
        remote_capabilities = set()

        for capa in message.split('\n'):
            if capa in CAPABILITIES:
                remote_capabilities.add(capa)

        return remote_capabilities

    def _write_chunk(self, type, sha, content):
        """Write content to pack, validating the parent if possible
        """
        assert(self.w.maybe_write(type, content))

    def _want_object_or_not(self, hash):
        """Tell if an object is needed or not. Checks in the repo and
        the hashes received in the exchange
        """
        if self.packList.exists(hash)\
           or self.validator.is_claimed(hash)\
           or self.w.exists(hash):
            return False
        else:
            return True

    def _prepare_next_messages(self, local_missing, remote_missing):

        if self.validator.is_over():
            allrefs = []
            for (refname, sha) in git.list_refs():
                allrefs.append(refname + ' ' + sha)
            for new_sha in self.new_commits:
                allrefs.append("newref" + ' ' + new_sha)
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

        allremotes = set()
        alllocal = set()
        for pair in self._decode_refs(data):
            if len(pair) == 2:
                allremotes.add(pair[1])
        for (name, sha) in git.list_refs():
            alllocal.add(sha)

        missing_here = allremotes - alllocal
        missing_there = alllocal - allremotes

        return missing_here, missing_there

    def _decode_refs(self, refs_message):
        allrefs = []
        for line in refs_message.split('\n'):
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

    def _end(self, allnewrefs):
        """Do all the cleaning operations. First step is to verify that
        we have all the chunks we were missing. We then properly close
        the packwriter, adding its data to the repo.
        """

        assert(self.validator.is_over())
        self.transfer_done = True

        if self.w.idx is not None and len(self.w.idx) > 0:
            log("writing tmp pack to repo...\n")

            self.w.close(run_midx=False)

            log("updating refs...\n")
            sum = Sha1(self.transport.getHost().host)
            sum.update(str(self.transport.getHost().port))
            remote_id = sum.digest().encode('hex')

            for (new_refname, sha) in allnewrefs:
                # strip refs/heads from the ref name
                ref_simple_name = re.sub("refs/heads/", '', new_refname)
                name = 'refs/heads/%s-%s' % (remote_id, ref_simple_name)

                old_sha = None
                for (refname, sha) in git.list_refs():
                    if new_refname == refname:
                        old_sha = sha

                git.update_ref(name, sha, old_sha)

            log("wrote to repo, have a nice day!\n")
        else:
            self.w.abort()
            log("no new data, skimming along\n")

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
