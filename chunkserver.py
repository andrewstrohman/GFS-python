#!/usr/bin/env python

import os, glob, collections, sys, zlib, time, xmlrpclib, threading
from ctypes import Structure, c_uint, c_int, c_ulong
import common
from common import debug



class LRUCacheNode(object):
    def __init__(self, serial, chunkID, chunkOffset, data):
        self.serial = serial
        self.chunkID = chunkID
        self.chunkOffset = chunkOffset
        self.data = data
        self.size = sys.getsizeof(LRUCacheNode) + len(data)


class LRUCache(object):
    def __init__(self):
        self.maxSize = 128 * 1024**2
        self.totalSize = 0
        self.hashMap = collections.OrderedDict()
        self.lock = threading.Lock()

    def add(self, LRUNode):
        with self.lock:
            if (LRUNode.chunkID, LRUNode.serial) in self.hashMap:
                #maybe version changed
                #debug("Duplicate LRU add() detected for chunk:%s, serial:%d, with size:%d"
                #    % (hex(LRUNode.chunkID), LRUNode.serial, LRUNode.size))
                self.totalSize -= self.hashMap[(LRUNode.chunkID, LRUNode.serial)].size 
            self.totalSize += LRUNode.size
            while self.totalSize > self.maxSize:
                (chunkID, serial), evicted = self.hashMap.popitem(last=False)
                self.totalSize -= evicted.size
            self.hashMap[(LRUNode.chunkID, LRUNode.serial)] = LRUNode

    def get(self, chunkID, serial):
        with self.lock:
            node = self.hashMap[(chunkID, serial)]
            #refresh
            del self.hashMap[(chunkID, serial)]
            self.hashMap[(chunkID, serial)] = node
            return node

    def setChunkOffset(self, chunkID, serial, chunkOffset):
        with self.lock:
            self.hashMap[(chunkID, serial)].chunkOffset = chunkOffset

class chunkMetadata(Structure):
    #size padded to power of 2 to align with disk blocks
    _pack_ = 1
    _fields_ = [
        ("ID", c_ulong),
        ("version", c_ulong),
        ("checksum", c_int),
        ("pad1", c_uint),
        ("pad2", c_ulong)]

    def __init__(self, ID, version, checksum):
        self.ID = ID
        self.version = version
        self.checksum = checksum

class chunk(object):
    def __init__(self, version, checksum, metaDataOffset):
        self.version = version
        self.checksum = checksum
        self.metaDataOffset = metaDataOffset
        self.leaseEndTime = 0
        self.serial = 0
        self.lock = threading.RLock()
        
class chunkserver(object):
    def __init__(self):
        self.chunksDir = "chunks"
        common.createDirectory(self.chunksDir)
        self.metadataFile = "metadata"
        self.chunks = {}
        self._loadMetaData()
        self._removeUnknownChunks()
        self.cache = LRUCache()

    def getChunk(self, chunkID, version):
        #should not need to lck becuase master took lease to do this
        #(only called by replicatFrom())
        if self.chunks[chunkID].version != version:
            raise common.UnexpectedChunkVersion
        data, checksum = self._getChecksum(chunkID)    
        if checksum != self.chunks[chunkID].checksum:
            debug("chunkID:%s expected checksum:%s, but current is :%s.  data len is:%d"
                % (hex(chunkID), hex(self.chunks[chunkID].checksum), hex(checksum),
                len(data)))
            self._deleteChunk(chunkID)
            raise common.BadChecksum
        return xmlrpclib.Binary(data)

    def replicateFrom(self, chunkID, version, replica):
        #should not need to lock because master took lease to do this
        binaryData =  common.ServerProxy('http://%s:8000' %
            replica).getChunk(chunkID, version).data
        self._writeDataToChunk(chunkID, 0, binaryData)
        self.updateChunkVersion(chunkID, version, 0, checksum=zlib.crc32(binaryData))

    def _leaseValid(self, chunkID):
        return int(time.time()) < self.chunks[chunkID].leaseEndTime - 3

    def loadWriteIntoPrimaryCache(self, chunkID, chunkOffset, data):
        #lock is to protect serial, not the data
        with self.chunks[chunkID].lock:
            if self._leaseValid(chunkID):
                self.chunks[chunkID].serial += 1
                self.cache.add(LRUCacheNode(self.chunks[chunkID].serial, chunkID,
                    chunkOffset, data.data))
                return self.chunks[chunkID].serial
            else:
                return None

    def loadWriteIntoCache(self, serial, chunkID, chunkOffset, data):
        self.cache.add(LRUCacheNode(serial, chunkID, chunkOffset, data.data))

    def _loadMetaData(self):
        common.createFile(self.metadataFile)
        with open(self.metadataFile, "rb") as FH:
            cm = chunkMetadata(0, 0, 0)
            while True:
                metaDataOffset = FH.tell()
                FH.readinto(cm)
                if metaDataOffset == FH.tell():
                    break
                #skip deleted chunks
                if cm.ID != 0:
                    self.chunks[cm.ID] = chunk(cm.version, cm.checksum, metaDataOffset)
                    debug("loaded chunkID:%s, version:%d, checksum:%s from metaDatOffset:%d" %\
                        (hex(cm.ID), cm.version, hex(cm.checksum), metaDataOffset))

    def _removeUnknownChunks(self):
        for chunkFile in glob.glob("%s/*.chunk" % self.chunksDir):
            if self._chunkFileNameToID(chunkFile) not in self.chunks:
                os.remove(chunkFile)

    def heartbeat(self):
        return {str(chunkID): chunkObj.version for chunkID, chunkObj in self.chunks.iteritems()}

    def updateChunkVersion(self, chunkID, version, leaseEndTime, checksum=0):
        if chunkID in self.chunks:
            self.chunks[chunkID].version = version
            self.chunks[chunkID].leaseEndTime = leaseEndTime
        else:
            self.chunks[chunkID] = chunk(version, checksum, os.path.getsize(self.metadataFile))
            self.chunks[chunkID].leaseEndTime = leaseEndTime
        metaDataOffset = self.chunks[chunkID].metaDataOffset
        with open(self.metadataFile, "r+b") as FH:
            FH.seek(metaDataOffset)
            FH.write(chunkMetadata(chunkID, version, self.chunks[chunkID].checksum))
            FH.flush()
            os.fsync(FH.fileno())

    def _deleteChunk(self, chunkID):
        metaDataOffset = self.chunks[chunkID].metaDataOffset
        del self.chunks[chunkID]
        with open(self.metadataFile, "r+b") as FH:
            FH.seek(metaDataOffset)
            FH.write(chunkMetadata(0, 0, 0))
            FH.flush()
            os.fsync(FH.fileno())
        try:
            os.remove(self._chunkIDToFileName(chunkID))
        except:
            pass

    def deleteChunks(self, chunks):
        for (chunkID, version) in chunks:
            if self.chunks[chunkID].version == version:
                self._deleteChunk(chunkID)

    def extendLease(self, chunks, leaseEndTime):
        for chunkID in chunks:
            self.chunks[chunkID].leaseEndTime = leaseEndTime

    def _getChecksum(self, chunkID):
        fileName = self._chunkIDToFileName(chunkID)
        try:
            with open(fileName, "r") as FH:
                data = FH.read()
                return data, zlib.crc32(data)
        except:
            return "", zlib.crc32("")
        

    def _updateChecksum(self, chunkID, checksum):
        self.chunks[chunkID].checksum = checksum
        with open(self.metadataFile, "r+b") as FH:
            FH.seek(self.chunks[chunkID].metaDataOffset)
            FH.write(chunkMetadata(chunkID, self.chunks[chunkID].version, checksum))
            FH.flush()
            os.fsync(FH.fileno())

    def _chunkIDToFileName(self, ID):
        return "%s/%s.chunk" % (self.chunksDir, hex(ID))

    def _chunkFileNameToID(self, filename):
        return int(os.path.basename(filename)[:-len(".chunk")], 16)

    def _getChunkSize(self, chunkID):
        try:
            with open(self._chunkIDToFileName(chunkID), "r+b") as FH:
                FH.seek(0, os.SEEK_END)
                return FH.tell()
        except:
            return 0
        

    def primaryAppend(self, chunkID, version, serial, replicas):
        with self.chunks[chunkID].lock:
            if not self._leaseValid(chunkID):
                debug("primaryAppend called, but don't have a valid lease.  chunkID:%s" % 
                    hex(chunkID))
                return common.NoLease
            debug("doing append as primary for chunkID:%s" % hex(chunkID))
            currSize = self._getChunkSize(chunkID)
            try:
                node = self.cache.get(chunkID, serial)
            except:
                raise common.ChunkNotFound
            if currSize + len(node.data) > common.chunkSize:
                ret = self._primaryPadChunk(chunkID, version, replicas)
                if ret == common.Success:
                    return common.RetryNextChunk
                else:
                    return ret
            self.writeChunkAtOffset(chunkID, version, serial, currSize)
            for replica in replicas:
                if not self._leaseValid(chunkID):
                    debug("primaryappend: invalid lease detected before")
                    return common.NoLease
                try:
                    common.ServerProxy('http://%s:8000' % replica
                        ).writeChunkAtOffset(chunkID, version, serial, currSize)
                except:
                    debug("primaryappend: secondary write failed for:%s" % replica)
                    return common.Fail
            return common.Success
            

    def _primaryPadChunk(self, chunkID, version, replicas):
        if not self._leaseValid(chunkID):
            return common.NoLease
        self.padChunk(chunkID, version)
        for replica in replicas:
            if not self._leaseValid(chunkID):
                return common.NoLease
            try:
                common.ServerProxy('http://%s:8000' % replica).padChunk(chunkID, version)
            except:
                return common.Fail
        return common.Success

    def padChunk(self, chunkID, version):
        currSize = self._getChunkSize(chunkID)
        padSize = common.chunkSize - currSize
        if padSize:
            #serial -1 is reserved for immediate writes
            self.loadWriteIntoCache(-1, chunkID, currSize, xmlrpclib.Binary('\0' * padSize))
            self.write(chunkID, version, -1)

    def writeChunkAtOffset(self, chunkID, version, serial, offset):
        self.cache.setChunkOffset(chunkID, serial, offset)
        self.write(chunkID, version, serial)
        

    def primaryWrite(self, chunkID, version, serial, replicas):
        with self.chunks[chunkID].lock:
            if not self._leaseValid(chunkID):
                debug("primaryWrite called, but don't have a valid lease.  chunkID:%s" % 
                    hex(chunkID))
                return False
            debug("doing write as primary for chunkID:%s" % hex(chunkID))
            self.write(chunkID, version, serial)
            for replica in replicas:
                if not self._leaseValid(chunkID):
                    debug("primaryWrite: invalid lease detected before")
                    return False
                if not common.ServerProxy('http://%s:8000' % replica
                        ).write(chunkID, version, serial):
                    debug("primaryWrite: secondary write failed for:%s" % replica)
                    return False
            return True


    def write(self, chunkID, version, serial):
        with self.chunks[chunkID].lock:
            if version != self.chunks[chunkID].version:
                raise common.UnexpectedChunkVersion 
            node = self.cache.get(chunkID, serial)
            data, checksum = self._getChecksum(node.chunkID)
            if checksum != self.chunks[node.chunkID].checksum:
                debug("chunkID:%s expected checksum:%s, but current is :%s.  data len is:%d"
                    % (hex(chunkID), hex(self.chunks[node.chunkID].checksum), hex(checksum),
                    len(data)))
                self._deleteChunk(chunkID)
                raise common.BadChecksum
            padSize = max(0, node.chunkOffset - len(data))
            newdata = data[:node.chunkOffset] + '\0' * padSize + node.data +\
                data[(node.chunkOffset + len(node.data)):]
            self._writeDataToChunk(chunkID, node.chunkOffset, node.data)
            self._updateChecksum(chunkID, zlib.crc32(newdata))
            return True

    def _writeDataToChunk(self, chunkID, dataOffset, data):
        chunkFileName = self._chunkIDToFileName(chunkID)
        common.createFile(chunkFileName)
        with open(chunkFileName, "r+b") as FH:
            FH.seek(dataOffset)
            FH.write(data)

    def read(self, chunkID, version, chunkOffset, size):
        with self.chunks[chunkID].lock:
            debug("received read request for chunkID:%s, version:%d, offset:%d, size:%d" %
                (hex(chunkID), version, chunkOffset, size))
            if version != self.chunks[chunkID].version:
                #debug("for chunkID:%s, on version:%d, but got read request for version %d" % 
                    #(hex(chunkID), self.chunks[chunkID].version, version))
                raise common.UnexpectedChunkVersion
            data, checksum = self._getChecksum(chunkID)
            if checksum != self.chunks[chunkID].checksum:
                debug("chunkID:%s expected checksum:%s, but current is :%s.  data len is:%d"
                    % (hex(chunkID), hex(self.chunks[chunkID].checksum), hex(checksum),
                    len(data)))
                self._deleteChunk(chunkID)
                raise common.BadChecksum
            return xmlrpclib.Binary(data[chunkOffset:size + chunkOffset])

def main():
    server = common.ThreadedXMLRPCServer(("", 8000), common.ExceptionHandler, False, True)
    server.register_introspection_functions()
    server.register_instance(chunkserver())
    server.serve_forever()

if __name__ == "__main__":
    main()
