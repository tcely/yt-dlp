from __future__ import annotations

import json
import os
import secrets
import socket
import subprocess
import threading
import time
from multiprocessing import shared_memory
from urllib.request import Request, urlopen


SHM_PREFIX = 'aria2c-rpc-status-'
SHM_SIZE = (1024) * 16  # KiB

SEQUENCE_OFFSET = 0
PAYLOAD_OFFSET = 1


class Aria2SharedStatus:
    def __init__(
        self,
        monitored_pid: int,
        *,
        create_server: bool = False,
    ):
        self.name = f'{SHM_PREFIX}{monitored_pid}'

        if create_server:
            try:
                self.shm = shared_memory.SharedMemory(
                    name=self.name,
                    create=True,
                    size=SHM_SIZE,
                )
            except FileExistsError:
                # Reuse the existing segment.
                self.shm = shared_memory.SharedMemory(
                    name=self.name,
                    create=False,
                )
        else:
            # Consumer paths only attach.
            self.shm = shared_memory.SharedMemory(
                name=self.name,
                create=False,
            )

    def _get_sequence(self) -> int:
        return self.shm.buf[SEQUENCE_OFFSET]

    def _set_sequence(self, value: int) -> None:
        self.shm.buf[SEQUENCE_OFFSET] = 0xFF & value

    def write(self, value: dict) -> None:
        payload = json.dumps(
            value,
            separators=(',', ':'),
        ).encode('utf-8')

        capacity = len(self.shm.buf) - PAYLOAD_OFFSET

        if len(payload) >= capacity:
            raise ValueError("aria2c status does not fit in shared memory")

        sequence = self._get_sequence()

        # Odd sequence means that a write is in progress.
        self._set_sequence(sequence | 1)

        self.shm.buf[PAYLOAD_OFFSET:] = b'\x00' * capacity
        self.shm.buf[
            PAYLOAD_OFFSET:(PAYLOAD_OFFSET + len(payload))
        ] = payload

        # Advance to the next even sequence.
        self._set_sequence(0xFE & (2 + sequence))

    def read(self) -> dict:
        data = None
        while data is None:
            before = self._get_sequence()
            # retry while writing
            if 1 == before & 1:
                continue

            payload = bytes(b
                self.shm.buf[PAYLOAD_OFFSET:]
            ).split(b"\x00", 1)[0]

            after = self._get_sequence()
            # retry because payload was overwritten
            if 1 == after & 1:
                continue

            if before == after:
                data = json.loads(payload.decode('utf-8')) if payload else {}
 
        return data

    def close(self) -> None:
        self.shm.close()
