#!/usr/bin/env python

import time, os, sys, random, string
from ctypes import sizeof
import record, common
from tests import message

ID = int(sys.argv[1])

FH = record.DuplicateDiscardRecordAppendFileHandle("master", "/test/producerConsumer", ID)

try:
    FH.create()
except:
    #in case file already exists
    pass


seqNum = 0
#structure messages in a way that a reader can verify
m = message.Message(ID)
#appends can be at most 1/4 of chunk size, including headers
maxSize = common.chunkSize/4 - sizeof(message.Message) - sizeof(record.SequenceHeader) -\
   sizeof(record.RecordHeader) 
while True:
    size = random.randint(1, maxSize)
    m.seqNum = seqNum
    m.len = size
    #pick random letter to be repeated size times
    m.char = random.choice(string.letters)
    #append the record
    FH.appendRecord(m.getBuffer() + m.char * m.len)
    seqNum += 1
