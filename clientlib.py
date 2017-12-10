#!/usr/bin/env python

import xmlrpclib, time, sys
import common
from common import debug


class Location(object):
    def __init__(self, chunkID, version, primary, secondaries):
        self.chunkID = chunkID
        self.version = version
        self.primary = primary
        self.secondaries = secondaries

class FileHandle(object):
    def __init__(self, masterName, path):
        #every RPC triggers fresh DNS resolution
        self.mp = common.ServerProxy('http://%s:8000' % masterName)
        self.path = path
        self.locationCache = {}

    def create(self):
        self.mp.create(self.path)

    def delete(self):
        self.mp.delete(self.path)

    def updateLocation(self, chunkIndex):
        while True:
            try:
                chunkID, version, primary, replicas = self.mp.findLocation(self.path,
                    chunkIndex)
                secondaries = []
                for replica in replicas:
                    if replica != primary:
                        secondaries.append(replica)
                self.locationCache[chunkIndex] = Location(chunkID, version, primary,
                    secondaries)
                return 
            except:
                pass
        

    def read(self, offset, size):
        ret = ""
        while True:
            chunkOffset = self._getChunkOffset(offset)
            chunkIndex = self._getChunkIndex(offset)
            if self._getCurrNumChunks() - 1 < chunkIndex:
                return ret
            if chunkIndex not in self.locationCache:
                self.updateLocation(chunkIndex)
            readSize = min(size, (common.chunkSize - chunkOffset))
            loc = self.locationCache[chunkIndex]
            for replica in loc.secondaries + [loc.primary]:
                p = common.ServerProxy('http://%s:8000' % replica)
                try:
                    ret +=  p.read(loc.chunkID, loc.version, chunkOffset, readSize).data
                    break
                except Exception as e:
                    pass
            else:
                #debug("read: no replica responded. retrying")
                time.sleep(1)
                self.updateLocation(chunkIndex)
                continue
            offset += readSize
            size -= readSize
            if not size:
                return ret 

    def write(self, offset, data):
        firstChunkOffset = self._getChunkOffset(offset)
        dataSize = len(data)
        firstChunkSize = min((common.chunkSize - firstChunkOffset), dataSize)
        self._writeToChunk(offset, data[:firstChunkSize])
        dataWritten = firstChunkSize
        while dataSize - dataWritten:
            chunkSize = min(common.chunkSize,  dataSize - dataWritten)
            self._writeToChunk(offset + dataWritten, data[dataWritten:dataWritten + chunkSize])
            dataWritten += chunkSize 

    def _writeToChunk(self, offset, data):
        encodedData = xmlrpclib.Binary(data)
        index = self._getChunkIndex(offset)
        if self._getCurrNumChunks() - 1 <  index:
            self._createChunk(index)
        if index not in self.locationCache:
            self.updateLocation(index)
        while True:
            try:
                loc = self.locationCache[index]
                serial = self._distributeWriteData(loc, self._getChunkOffset(offset),
                    encodedData)
                common.ServerProxy('http://%s:8000' % loc.primary).primaryWrite(
                    loc.chunkID, loc.version, serial, loc.secondaries)
                return
            except Exception as e:
                time.sleep(1)
                self.updateLocation(index)

    def _distributeWriteData(self, loc, chunkOffset, encodedData):
        serial = common.ServerProxy('http://%s:8000' % loc.primary
            ).loadWriteIntoPrimaryCache(loc.chunkID, chunkOffset, encodedData)
        if serial is None:
            raise common.PrimaryLoadWriteFailure
        debug("primary:%s has given me serial:%d for write to chunkID:%s" %
            (loc.primary, serial, hex(loc.chunkID)))
        for replica in loc.secondaries:
            common.ServerProxy('http://%s:8000' % replica
                ).loadWriteIntoCache(serial, loc.chunkID, chunkOffset, encodedData)
        return serial

    def _getCurrNumChunks(self):
        while True:
            try:
                return self.mp.getNumChunks(self.path)
            except Exception as e:
                time.sleep(1)

    def _createChunk(self, index):
        while True:
            try:
                self.mp.createChunk(self.path, index)
                return
            except:
                time.sleep(1)
        
    def append(self, data):
        if len(data) > (common.chunkSize/4.0):
            raise common.AppendTooLarge
        currNumChunks = self._getCurrNumChunks()
        encodedData = xmlrpclib.Binary(data)
        chunkIndex = currNumChunks - 1
        if not currNumChunks:
            self._createChunk(0)
            chunkIndex = 0
        while True:
            if chunkIndex not in self.locationCache:
                self.updateLocation(chunkIndex)
            loc = self.locationCache[chunkIndex]
            try:
                #chunk offset doesn't matter, because it will be changed
                serial = self._distributeWriteData(loc, -1, encodedData)
                ret = common.ServerProxy('http://%s:8000' % loc.primary).primaryAppend(
                    loc.chunkID, loc.version, serial, loc.secondaries)
                if ret == common.Success:
                    return
                elif ret == common.RetryNextChunk:
                    chunkIndex += 1
                    self._createChunk(chunkIndex)
                else:
                    time.sleep(1)
                    self.updateLocation(chunkIndex)
            except Exception as e:
                time.sleep(1)
                self.updateLocation(chunkIndex)
        

    def _getChunkIndex(self, offset):
        return offset/common.chunkSize

    def _getChunkOffset(self, offset):
        return offset % common.chunkSize
