#!/usr/bin/env python

import time, os, sys
import record
from ctypes import sizeof
from tests import message

#argument passed is the producer ID to follow
ID = int(sys.argv[1])

FH = record.DuplicateDiscardRecordAppendFileHandle("master", "/test/producerConsumer")

expected = 0
m = message.Message(ID)
while True: 
    data = FH.getNextRecord() 
    if data is None:
        time.sleep(1)
        continue
    m.cast(data)
    #we are only tracking one producer
    if m.ID == ID:
        #check that seqNum is correct.
        if m.seqNum != expected:
            print "expected:%d, but got:%d" % (expected, m.seqNum)
            break
        #check that the message contenet is correct
        if data[sizeof(message.Message):] != m.char * m.len:
            print "data corruption!"
            break
        #report
        print "found:%d" % expected
        expected += 1 
