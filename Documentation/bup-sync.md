% bup-sync(1) Bup %BUP_VERSION%
% Matthieu Rakotojaona <matthieu.rakotojaona@gmail.com>
% %BUP_DATE%

# NAME

bup-sync - synchronises repositories accross network

# SYNOPSIS

bup sync --repo /path/to/repo --port port [--remote_host rhost]
\[--remote_port rport] \[--push] \[--pull]

# DESCRIPTION

`bup sync` efficiently synchronises two bup repositories accross between
two machines on a TCP/IP network. It leverages the organisation of a bup
repository by only exchanging useful data, at the cost of a chatty and
poorly predictable conversation.

The protocol for exchanging chunks of data is composed of frames. They
are exchanged on raw TCP sockets. If you want more security, you should
consider creating an SSH tunnel.

More documentation on the internals of the protocol is available in the
SYNC document.

# OPTIONS

--repo */path/to/repo*
:   the path to the repository used.

--port *port*
:   the *port* to listen to.

--remote_host *hostname*
:   the *hostname* to connect to. Must be resolvable by standard internet ways
    (i.e. ip address or DNS name)

--remote_port *port*
:   the port to connect to.

--push 
:   if set, indicates we want to push data to the remote peer, and that
    we are ok with pushing data the other sides wants to receive.

--pull 
:   if set, indicates we want to pull data from the remote peer, and
    that we are ok with pulling data the other side wants to send.
