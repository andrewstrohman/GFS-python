#!/usr/bin/env python


import xmlrpclib, random
import socket, threading, time, signal, os, sys, ctypes
from ctypes import Structure, c_ulong
import common
from common import debug

class Directory(object):
    def __init__(self, name):
        self.name = name
        self.subDirectories = {}
        self.files = {}

class File(object):
    def __init__(self, name):
        self.name = name
        self.chunks = []
        
class Chunk(object):
    def __init__(self, version, offset):
        self.version = version
        self.offset = offset
        # replica -> time it was told about last version update
        self.replicas = {}
        self.leaseHolder = None
        self.leaseTimeOut = 0
        self.masterWantsLease = False
        self.lock = threading.Lock()

class ChunkServer(object):
    def __init__(self, IP):
        self.unreachableCount = 0
        self.numChunks = 0
        self.IP = IP
        self.chunks = []
        self.lastChunksUpdate = 0
        self.lock = threading.Lock()


class chunkMetadata(Structure):
    _pack_ = 1
    _fields_ = [
        ("ID", c_ulong),
        ("version", c_ulong)]

    def __init__(self, ID, version):
        self.ID = ID
        self.version = version

class master(object):
    def __init__(self):
        self.FSLock = threading.Lock()
        self.currChunkID = 0 
        self.metaData = Directory('/')
        self.metadataFileName = "metadata"
        self.filesDirectory = "files"
        if self.filesDirectory.endswith("/"):
            self.filesDirectory = self.filesDirectory[:-1]
        self._loadFilesMetaData()
        self.files = {}
        self.chunks = {}
        self.missingReplicaTally = {}
        self._loadChunkMetaData()
        self.heartbeatThreads = {}
        self.heartbeatInterval = 20
        self.terminate = False
        signal.signal(signal.SIGINT, self._catch)
        self.chunkservers = {}
        for chunkserver in socket.gethostbyname_ex("chunkservers")[2]:
            self.chunkservers[chunkserver] = ChunkServer(chunkserver)
            t = threading.Thread(target=self._doHeartbeat,
                args=(self.chunkservers[chunkserver],))
            self.heartbeatThreads[chunkserver] = t
        self._initializeReplicas(self.chunkservers.keys())
        for thread in self.heartbeatThreads.values():
            thread.start()
        threading.Thread(target=self._replicationThread, args=(None,)).start()

    def _replicationThread(self, nothing):
        lastCheck = 0
        while True:
            if self.terminate:
                return
            time.sleep(.5)
            if int(time.time()) - lastCheck > self.heartbeatInterval:
                self._checkReplication()
                lastCheck = int(time.time())

    def _checkReplication(self):
        updateVersion = []
        replicate = []
        for chunkID, chunkInfo in self.chunks.iteritems():
            numReplicas = len(chunkInfo.replicas)
            for replica in chunkInfo.replicas.keys():
                with self.chunkservers[replica].lock:
                    if (chunkID not in self.chunkservers[replica].chunks and\
                            self.chunkservers[replica].lastChunksUpdate >\
                            self.chunks[chunkID].replicas[replica]) or\
                            self.chunkservers[replica].unreachableCount > 1:
                        debug("chunkID:%d lost replica:%s" % (chunkID, replica))
                        debug("replica reports chunk:%s, unreachable count:%d" %
                            (chunkID in  self.chunkservers[replica].chunks,
                            self.chunkservers[replica].unreachableCount))
                        numReplicas -= 1
                        #update version so we can write
                        #with a reduced replica group
                        if chunkID not in updateVersion:
                            updateVersion.append(chunkID)
            #we have a replica group that is too small, but all members are available
            #let the master hold the lease to re-replicate
            if numReplicas < common.replicationLevel and chunkID not in updateVersion:
                self.chunks[chunkID].masterWantsLease = True
                replicate.append(chunkID)
        
        #try to update lease version for groups who
        #have lost members
        for chunkID in updateVersion:
            try:
                with self.chunks[chunkID].lock:
                    self._updateChunkVersion(chunkID, forMaster=True)
            except:
                pass

        for chunkID in replicate:
            try:
                if self.chunks[chunkID].leaseTimeOut < int(time.time()):
                    for newReplica in self._chunkServersByUsage():
                        if newReplica in self.chunks[chunkID].replicas:
                            continue
                        for existingReplica in self.chunks[chunkID].replicas.keys():
                            try:
                                common.ServerProxy('http://%s:8000' %
                                    newReplica).replicateFrom(chunkID,
                                    self.chunks[chunkID].version, existingReplica)
                                self.chunks[chunkID].replicas[newReplica] = time.time()
                                debug("chunkID:%s replicated to:%s from:%s" % (hex(chunkID),
                                    newReplica, existingReplica))
                                break
                            except Exception as e:
                                pass
                        if len(self.chunks[chunkID].replicas) == common.replicationLevel:
                            break
                    self.chunks[chunkID].masterWantsLease = False
            except:
                #chunk may be been deleted
                pass


    def _chunkServersByUsage(self):
        ret = []
        for chunkserver in sorted(self.chunkservers.values(), key=lambda cs: cs.numChunks):
            if not chunkserver.unreachableCount:
                ret.append(chunkserver.IP)
        return ret

    def getNumChunks(self, path):
        with self.FSLock:
            return len(self._findFile(path).chunks)
        

    def createChunk(self, path, index):
        with self.FSLock:
            if len(self._findFile(path).chunks) >= (index + 1):
                return
            self.currChunkID += 1
            newChunk = Chunk(-1, os.path.getsize(self.metadataFileName))
            with newChunk.lock:
                self.chunks[self.currChunkID] = newChunk
                newChunk.replicas = {key: 0 for key in
                    self._chunkServersByUsage()[:common.replicationLevel]}
                self._updateChunkVersion(self.currChunkID)
                self._appendChunkToFile(path)

    def _updateChunk(self, chunkID, version, holder, leaseTimeOut):
        self.chunks[chunkID].leaseHolder = holder
        self.chunks[chunkID].leaseTimeOut = leaseTimeOut
        if version != self.chunks[chunkID].version:
            self.chunks[chunkID].version = version
            with open(self.metadataFileName, "r+b") as FH:
                FH.seek(self.chunks[chunkID].offset)
                FH.write(chunkMetadata(chunkID,version))
                FH.flush()
                os.fsync(FH.fileno())

    def _delChunk(self, chunkID):
        with open(self.metadataFileName, "r+b") as FH:
            FH.seek(self.chunks[chunkID].offset)
            FH.write(chunkMetadata(0,0))
            FH.flush()
            os.fsync(FH.fileno())
        del self.chunks[chunkID]

    def _loadChunkMetaData(self):
        common.createFile(self.metadataFileName)
        with open(self.metadataFileName, "rb") as FH:
            cm = chunkMetadata(0, 0)
            while True:
                offset = FH.tell()
                FH.readinto(cm)
                if offset == FH.tell():
                    break
                if cm.ID == 0:
                    continue
                self.chunks[cm.ID] = Chunk(cm.version, offset)
                if cm.ID > self.currChunkID:
                    self.currChunkID = cm.ID

    def _loadFilesMetaData(self):
        common.createDirectory(self.filesDirectory)
        for root, dirs, files in os.walk(self.filesDirectory):
            for filename in files:
                filePath = os.path.join(root, filename)
                self.create(filePath)
                f = self._findFile(filePath[len(self.filesDirectory):])
                with open(filePath, "rb") as FH:
                    while True:
                        offset = FH.tell()
                        chunkID = ctypes.c_ulong(0)
                        FH.readinto(chunkID)
                        if offset == FH.tell():
                            break
                        f.chunks.append(chunkID.value)

    def _getMetaDataFilePath(self, fileName):
        return os.path.join(self.filesDirectory, fileName[1:])

    def _appendChunkToFile(self, fileName):
        f = self._findFile(fileName)
        f.chunks.append(self.currChunkID)
        with open(self._getMetaDataFilePath(fileName), "ab") as FH:
            FH.write(ctypes.c_ulong(self.currChunkID))
            FH.flush()
            os.fsync(FH.fileno())
        

    def _catch(self, signum, frame):
        self.terminate = True
        for t in self.heartbeatThreads.values():
            t.join()
        sys.exit(0)

    def _initializeReplicas(self, chunkservers):
        for chunkserver in chunkservers:
            try:
                for chunkID, version in common.ServerProxy('http://%s:8000' %
                        chunkserver).heartbeat().iteritems():
                    #dictionary key must be string for RPC
                    chunkID = int(chunkID)
                    if chunkID in self.chunks:
                        if version > self.chunks[chunkID].version:
                            #we must have died after granting a lease, but
                            #before committing the version to stable storage
                            #Let's conservatively wait the whole lease period before
                            #granting a new lease
                            self._updateChunk(chunkID, version, None, 0)
                            self.chunks[chunkID].replicas = {}
                        if version == self.chunks[chunkID].version:
                            self.chunks[chunkID].replicas[chunkserver] = 0
            except socket.error:
                pass

    def _doHeartbeat(self, chunkserver):
        cs = common.ServerProxy('http://%s:8000' % chunkserver.IP)
        lastBeat = 0
        while True:
            time.sleep(.5)
            if self.terminate:
                return
            if int(time.time()) - lastBeat > self.heartbeatInterval:
                try:
                    numChunks = 0
                    chunksToBeDeleted = []
                    leaseExtendedChunks = []
                    chunkInfo = cs.heartbeat()
                    with chunkserver.lock:
                        chunkserver.lastChunksUpdate = time.time()
                        chunkserver.chunks = []
                        for chunkID, version in chunkInfo.iteritems():
                            #dictionary key must be string for RPC
                            chunkID = int(chunkID)
                            if chunkID not in self.chunks or version <\
                                    self.chunks[chunkID].version:
                                #TODO tell chunkserver to remove bad chunks
                                continue
                            chunkserver.chunks.append(chunkID)
                            if self.chunks[chunkID].leaseHolder == chunkserver.IP and not\
                                    self.chunks[chunkID].masterWantsLease:
                                leaseExtendedChunks.append(chunkID)
                            numChunks += 1
                        chunkserver.numChunks = numChunks
                    if chunksToBeDeleted:
                        debug("telling %s to delete chunks:%s" % (chunkserver.IP,
                            chunksToBeDeleted))
                        cs.deleteChunks(chunksToBeDeleted)
                    if leaseExtendedChunks:
                        newLeaseEndTime = int(time.time()) + common.leasePeriod
                        debug("telling %s to extend lease until:%d for chunks:%s" %
                            (chunkserver.IP, newLeaseEndTime, leaseExtendedChunks))
                        cs.extendLease(leaseExtendedChunks, newLeaseEndTime)
                        for chunkID in leaseExtendedChunks:
                            self.chunks[chunkID].leaseTimeOut = newLeaseEndTime
                    chunkserver.unreachableCount = 0
                except socket.error:
                    chunkserver.unreachableCount += 1
                lastBeat = time.time()

    def _findFile(self, path):
        d, f = os.path.split(path)
        currDir = self.metaData
        for direct in d.split('/')[1:]:
            if direct not in currDir.subDirectories:
                raise common.FileNotFound
            currDir = currDir.subDirectories[direct]
        if f not in currDir.files:
            raise common.FileNotFound
        return currDir.files[f]

    def create(self, path):
        with self.FSLock:
            try:
                self._findFile(path)
            except common.FileNotFound:
                d, f = os.path.split(path)
                currDir = self.metaData
                for direct in d.split('/')[1:]:
                    if direct not in currDir.subDirectories:
                        currDir.subDirectories[direct] = Directory(direct)
                    currDir = currDir.subDirectories[direct]
                currDir.files[f] = File(f)
                filePath = self._getMetaDataFilePath(path)
                common.createDirectory(os.path.dirname(filePath))
                common.createFile(filePath)
            else:
                raise common.FileExists

    
    def delete(self, path):
        with self.FSLock:
            currDir = self.metaData
            d, f = os.path.split(path)
            for direct in d.split('/')[1:]:
                if direct not in currDir.subDirectories:
                    raise common.FileNotFound
                currDir = currDir.subDirectories[direct]
            if f not in currDir.files:
                raise common.FileNotFound
            else:
                os.remove(self._getMetaDataFilePath(path))
                for chunkID in currDir.files[f].chunks:
                    self._delChunk(chunkID)
                del currDir.files[f]
                

    def findLocation(self, path, chunkIndex):
        f = self._findFile(path)
        if len(f.chunks) <= chunkIndex:
            raise common.ChunkNotFound
        chunkID = f.chunks[chunkIndex]
        if self.chunks[chunkID].leaseHolder is not None and\
                self.chunks[chunkID].leaseTimeOut > int(time.time()):
            pass
        else:
            while True:
                if self.chunks[chunkID].masterWantsLease:
                    time.sleep(1)
                else:
                    with self.chunks[chunkID].lock:
                        self._updateChunkVersion(chunkID)
                    break
        replicas = self.chunks[chunkID].replicas.keys()
        random.shuffle(replicas)
        return chunkID, self.chunks[chunkID].version, self.chunks[chunkID].leaseHolder,\
            replicas

    def _updateChunkVersion(self, chunkID, forMaster=False):
        if self.chunks[chunkID].masterWantsLease and not forMaster:
            return
        newVersion = self.chunks[chunkID].version + 1
        endLeaseTime = int(time.time()) + common.leasePeriod
        #the new holder, yet to be assigned
        holder = None
        aliveReplicas = {}
        for replica in self.chunks[chunkID].replicas.keys():
            try:
                cs = common.ServerProxy('http://%s:8000' % replica)
                cs.updateChunkVersion(chunkID, newVersion, 0 if holder or forMaster
                    else endLeaseTime)
                if forMaster:
                    debug("informed:%s that master wants lease for chunkID:%s, version:%d"
                        % (replica, hex(chunkID), newVersion))
                elif holder is None:
                    debug("granted new lease to:%s for chunkID:%s, version:%d" %
                        (replica, hex(chunkID), newVersion))
                    holder = replica
                else:
                    debug("informed secondary:%s that chunkID:%s is now at version:%d" %
                        (replica, hex(chunkID), newVersion))
                aliveReplicas[replica] = time.time()
            except Exception as e:
                debug("unable to update chunkID:%s, to version:%d on replica:%s" % (
                    hex(chunkID), newVersion, replica))
        if not aliveReplicas:
            raise common.NoReplicaAvailable
        else:
            self._updateChunk(chunkID, newVersion, holder, 0 if forMaster else endLeaseTime)
            self.chunks[chunkID].replicas = aliveReplicas
        
server = common.ThreadedXMLRPCServer(("", 8000), common.ExceptionHandler, False, True)
server.register_introspection_functions()
server.register_instance(master())
server.serve_forever()
