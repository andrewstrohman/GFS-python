#!/usr/bin/env python

import os, SocketServer, traceback, httplib, socket, xmlrpclib
from SimpleXMLRPCServer import SimpleXMLRPCServer, SimpleXMLRPCRequestHandler
from SimpleXMLRPCServer import resolve_dotted_attribute

chunkSize = 32 * 1024**2
#chunkSize = 8 * 1024**2
replicationLevel = 3
leasePeriod = 60
DEBUG = True

RetryNextChunk, Success, Fail, NoLease = range(4)

class FileNotFound(Exception):
    pass

class FileExists(Exception):
    pass

class ChunkNotFound(Exception):
    pass

class UnexpectedChunkVersion(Exception):
    pass

class BadChecksum(Exception):
    pass

class NoReplicaAvailable(Exception):
    pass

class AppendTooLarge(Exception):
    pass

class PrimaryLoadWriteFailure(Exception):
    pass

def createFile(fileName):
    if not os.path.isfile(fileName):
        with open(fileName, "a"):
            pass

def createDirectory(directoryName):
    try:
        os.makedirs(directoryName)
    except os.error:
        pass

class ThreadedXMLRPCServer(SocketServer.ThreadingMixIn,SimpleXMLRPCServer):
    pass
#class ThreadedXMLRPCServer(SimpleXMLRPCServer):
#    pass


#So call stacks are dumped server side
class ExceptionHandler(SimpleXMLRPCRequestHandler):
    #taken from SimpleXMLRPCServer.py
    #and modified self. -> self.server.
    def _dispatch(self, method, params):
        try:
            func = None
            try:
                # check to see if a matching function has been registered
                func = self.server.funcs[method]
            except KeyError:
                if self.server.instance is not None:
                    # check for a _dispatch method
                    if hasattr(self.server.instance, '_dispatch'):
                        return self.server.instance._dispatch(method, params)
                    else:
                        # call instance method directly
                        try:
                            func = resolve_dotted_attribute(
                                self.server.instance,
                                method,
                                self.server.allow_dotted_names
                                )
                        except AttributeError:
                            pass

            if func is not None:
                return func(*params)
            else:
                raise Exception('method "%s" is not supported' % method)
        except:
            debug(traceback.format_exc())
            raise

class HTTPConnectionKeepAlive(httplib.HTTPConnection):
    def connect(self):
        self.sock = self._create_connection((self.host,self.port),
            self.timeout, self.source_address)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        self.sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPIDLE, 20)
        self.sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPCNT, 4)
        self.sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPINTVL, 5)
        if self._tunnel_host:
            self._tunnel()

class KeepAliveTransport(xmlrpclib.Transport):
    def make_connection(self, host):
        if self._connection and host == self._connection[0]:
            return self._connection[1]
        chost, self._extra_headers, x509 = self.get_host_info(host)
        self._connection = host, HTTPConnectionKeepAlive(chost)
        return self._connection[1]

def ServerProxy(target):
    return xmlrpclib.ServerProxy(target, transport=KeepAliveTransport()) 

def debug(msg):
    if DEBUG:
        print msg
