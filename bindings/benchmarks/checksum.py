# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Type-sensitive cross-language result oracle (HDBBOUND1)."""

import struct


class Checksum:
    def __init__(self, columns):
        self.state = 0xCBF29CE484222325
        self.bytes(b"HDBBOUND1")
        self.length(len(columns))
        for column in columns:
            self.value(column)

    def bytes(self, data):
        for byte in data:
            self.state = ((self.state ^ byte) * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF

    def length(self, length):
        self.bytes(struct.pack("<Q", length))

    def value(self, value):
        if value is None:
            self.bytes(b"\x00")
        elif isinstance(value, bool):
            self.bytes(bytes([1, int(value)]))
        elif isinstance(value, int):
            self.bytes(b"\x02" + struct.pack("<q", value))
        elif isinstance(value, float):
            self.bytes(b"\x03" + struct.pack("<d", value))
        elif isinstance(value, str):
            data = value.encode("utf-8")
            self.bytes(b"\x04")
            self.length(len(data))
            self.bytes(data)
        elif isinstance(value, bytes):
            self.bytes(b"\x05")
            self.length(len(value))
            self.bytes(value)
        elif isinstance(value, list):
            self.bytes(b"\x07")
            self.length(len(value))
            for item in value:
                self.value(item)
        elif isinstance(value, dict):
            self.bytes(b"\x08")
            self.length(len(value))
            for key in sorted(value):
                self.value(key)
                self.value(value[key])
        else:
            raise TypeError("unexpected result type: " + type(value).__name__)

    def row(self, values):
        self.bytes(b"\xff")
        self.length(len(values))
        for value in values:
            self.value(value)

    def hex(self):
        return "%016x" % self.state
