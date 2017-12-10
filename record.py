#!/usr/bin/env python

from ctypes import Structure, c_uint, c_ulong, c_char, sizeof, memmove, addressof
import zlib
from clientlib import FileHandle
import common
from common import debug


class BufferStructure(Structure):
    _pack_ = 1

    def cast(self, data):
        memmove(addressof(self), data, min(sizeof(self), len(data)))

    def getBuffer(self):
        return buffer(self)[:]
     

class RecordHeader(BufferStructure):
    _fields_ = [
        ("BOR", c_char * 4),
        ("len", c_uint),
        ("checksum", c_uint)]

    def __init__(self, length=0, checksum=0):
        self.BOR = "BOR"
        self.len = length
        self.checksum = checksum

       


class RecordAppendFileHandle(FileHandle):
    def __init__(self, masterName, path):
        super(RecordAppendFileHandle, self).__init__(masterName, path)
        #offset within the file we are scanning for next record
        self.scanOffset = 0
        #chunk Index we are currently reading from
        self.chunkIndexRead = 0
        #data read from chunk current chunk.
        #When closely following an appender, we may have to keep trying to grab more
        #data from a chunk as it is getting filled in
        self.data = ""
        
    def appendRecord(self, data):
        self.append(RecordHeader(len(data), zlib.crc32(data)).getBuffer() + data)

    def lastChunkReadComplete(self):
        #there may be padding but we can count on full chunks
        return len(self.data) == common.chunkSize

    def readMoreData(self):
        #returns true when able to read some more data
        if self.lastChunkReadComplete():
            #we are at chunk boundry.  Read next chunk
            self.chunkIndexRead += 1
            self.data = self.read(common.chunkSize * self.chunkIndexRead, common.chunkSize)
            return bool(len(self.data))
        else:
            #continue trying to get more data from current chunk
            currAmountRead = len(self.data)
            self.data += self.read(common.chunkSize * self.chunkIndexRead + currAmountRead,
                common.chunkSize - currAmountRead)
            return len(self.data) != currAmountRead

    def scanForNextRecord(self):
        chunkOffset = self._getChunkOffset(self.scanOffset)
        rh = RecordHeader()
        while True:
            chunkOffset = self.data.find("BOR\0", chunkOffset)
            if chunkOffset == -1:
                #if we are at the end of a chunk move scan offset to beginning
                #of next chunk
                if self.lastChunkReadComplete():
                    self.scanOffset += (common.chunkSize -
                        self._getChunkOffset(self.scanOffset))
                return None
            if len(self.data[chunkOffset:]) < sizeof(RecordHeader):
                return None
            else:
                rh.cast(self.data[chunkOffset:])
                if len(self.data[chunkOffset:]) < sizeof(RecordHeader) + rh.len:
                    return None
                dataOffset = chunkOffset + sizeof(RecordHeader)
                dataEnd = dataOffset + rh.len
                recordData = self.data[dataOffset:dataEnd]
                if c_uint(zlib.crc32(recordData)).value == rh.checksum:
                    self.scanOffset = self.chunkIndexRead * common.chunkSize + dataEnd
                    return recordData


    def getNextRecord(self):
        while True:
            #first try to get try to find record in already read data
            record = self.scanForNextRecord()
            if record:
                return record
            #record not in already read data.  Try to read more data
            if not self.readMoreData():
                return None

class SequenceHeader(BufferStructure):
    _fields_ = [
        ("ID", c_uint),
        ("seqNum", c_ulong)]

    def __init__(self, ID=0, seqNum=0):
        self.ID = ID
        self.seqNum = seqNum

class DuplicateDiscardRecordAppendFileHandle(RecordAppendFileHandle):
    def __init__(self, masterName, path, ID=0):
        super(DuplicateDiscardRecordAppendFileHandle, self).__init__(masterName, path)
        self.expectedSeqNums = {}
        self.seqNum = 0
        self.ID = ID
        
    def appendRecord(self, data):
        super(DuplicateDiscardRecordAppendFileHandle, self).appendRecord(SequenceHeader(
            self.ID,
            self.seqNum).getBuffer() + data)
        self.seqNum += 1

    def getNextRecord(self):
        sh = SequenceHeader()
        while True:
            record = super(DuplicateDiscardRecordAppendFileHandle, self).getNextRecord()
            if record is None:
                return None
            sh.cast(record)
            if sh.ID not in self.expectedSeqNums:
                self.expectedSeqNums[sh.ID] = 0
            if sh.seqNum != self.expectedSeqNums[sh.ID]:
                debug("expected %d from %d, but got %d. discarding" % (
                    self.expectedSeqNums[sh.ID],
                    sh.ID, sh.seqNum))
                continue
            self.expectedSeqNums[sh.ID] += 1 
            return record[sizeof(SequenceHeader):]

