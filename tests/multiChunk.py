#!/usr/bin/env python

import clientlib, common


FH = clientlib.FileHandle("master", "/test/multiChunk")

try:
    FH.delete()
except:
    pass

FH.create()
#write half a chunk of A
FH.write(0, "A" * (common.chunkSize/2))
#this goes over chunk boundry
FH.write((common.chunkSize/2), "B" * common.chunkSize)
#and again
FH.write((3*common.chunkSize/2), "C" * common.chunkSize * 2)
#this reads the 3.5 chunks we just wrote
data = FH.read(0, 7 * common.chunkSize / 2)
if data == "A" * (common.chunkSize/2) + "B" * common.chunkSize + "C" * common.chunkSize * 2:
    print "success"
#this reads 3 chunks starting starting at offset .5 * (chunk size)
data = FH.read(common.chunkSize / 2, common.chunkSize * 3)
if data == "B" * common.chunkSize + "C" * common.chunkSize * 2:
    print "success"
#now delete the file
FH.delete()
