#!/usr/bin/env python

from ctypes import Structure, c_uint, c_ulong, c_char
import record



class Message(record.BufferStructure):
    _fields_ = [
        ("ID", c_uint),
        ("seqNum", c_ulong),
        ("len", c_uint),
        ("char", c_char)]

    def __init_(self, ID):
        self.ID = ID
