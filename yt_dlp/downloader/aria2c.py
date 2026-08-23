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


def get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_for_rpc(
    process: subprocess.Popen,
    port: int,
    timeout: float = 5,
) -> None:
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(
                f"aria2c exited with status {process.returncode}"
            )

        try:
            with socket.create_connection(
                ("127.0.0.1", port),
                timeout=0.2,
            ):
                return
        except OSError:
            time.sleep(0.05)

    raise TimeoutError("aria2c RPC server did not become ready")


def aria2_rpc_thread(
    monitored_pid: int,
    ready: threading.Event,
) -> None:
    # This is the only creation path for the shared-memory segment.
    status = Aria2SharedStatus(
        monitored_pid,
        create_server=True,
    )

    secret = secrets.token_urlsafe(32)
    port = get_free_port()

    rpc_url = f'http://127.0.0.1:{port}/jsonrpc'
    rpc_token = f'token:{secret}'

    state = {
        'state': 'starting',

        # This is the PID aria2c monitors.
        'monitored_pid': monitored_pid,

        'aria2_pid': None,

        'shared_memory_name': status.name,
        'shared_memory_size': len(status.shm.buf),

        'rpc_host': '127.0.0.1',
        'rpc_port': port,
        'rpc_url': rpc_url,
        'secret': secret,
        'rpc_token': rpc_token,

        'started_at': time.monotonic(),
    }

    status.write(state)

    aria2 = None

    try:
        aria2 = subprocess.Popen([
            'aria2c',
            '--no-conf', '--daemon=false',

            '--enable-rpc=true',
            '--rpc-listen-address=127.0.0.1',
            f'--rpc-listen-port={port}',
            f'--rpc-secret={secret}',

            # aria2c exits when the Python process exits.
            f'--stop-with-process={monitored_pid}',
        ])

        state['aria2_pid'] = aria2.pid
        status.write(state)

        wait_for_rpc(aria2, port)

        state['state'] = 'ready'
        state['ready_at'] = time.monotonic()
        status.write(state)

        ready.set()

        # Keep the daemon thread alive while aria2c runs.
        aria2.wait()

    except Exception as exc:
        error_at = time.monotonic()
        first_error_at = state.get('error_at', error_at)
        state['state'] = 'error'
        state['error'] = repr(exc)
        state['error_at'] = error_at
        state['first_error_at'] = first_error_at
        status.write(state)
        ready.set()

        if aria2 is not None and aria2.poll() is None:
            aria2.terminate()

    finally:
        state['state'] = 'stopped'
        state['stopped_at'] = time.monotonic()
        status.write(state)

        if aria2 is not None:
            try:
                aria2.wait(timeout=2)
            except subprocess.TimeoutExpired:
                aria2.kill()
                aria2.wait()

        status.close()


monitored_pid = os.getpid()
rpc_ready = threading.Event()

rpc_thread = threading.Thread(
    target=aria2_rpc_thread,
    args=(monitored_pid, rpc_ready),
    name='aria2c-rpc-server',
    daemon=True,
)

rpc_thread.start()


def get_rpc_status(monitored_pid: int, timeout: float = 10.0) -> Aria2SharedStatus:
    status = Aria2SharedStatus(
        monitored_pid,
        create_server=False,
    )

    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        current = status.read()

        if 'ready' == current.get('state'):
            return status

        if 'error' == current.get("state"):
            error = current.get('error', 'aria2c failed to start')
            status.close()
            raise RuntimeError(error)

        time.sleep(0.05)

    status.close()
    raise TimeoutError('Timed out waiting for aria2c RPC')


def rpc_call(
    status: Aria2SharedStatus,
    method: str,
    params: list,
):
    connection = status.read()

    request_body = json.dumps({
        'jsonrpc': '2.0',
        'id': secrets.token_hex(8),
        'method': method,
        'params': [
            connection['rpc_token'],
            *params,
        ],
    }).encode('utf-8')

    request = Request(
        connection['rpc_url'],
        data=request_body,
        headers={
            'Content-Type': 'application/json',
        },
        method='POST',
    )

    with urlopen(request, timeout=30) as response:
        result = json.load(response)

    if 'error' in result:
        raise RuntimeError(result['error'])

    return result['result']


status = get_rpc_status(monitored_pid)

gid = rpc_call(
    status,
    'aria2.addUri',
    [
        ['https://example.com/file.zip'],
        {
            'dir': '/tmp/downloads',
            'continue': 'true',
        },
    ],
)

print('Started transfer:', gid)


gids = []

for url in urls:
    gid = rpc_call(
        status,
        'aria2.addUri',
        [[url]],
    )
    gids.append(gid)


def monitor_transfer(
    status: Aria2SharedStatus,
    gid: str,
    interval: float = 1.0,
) -> dict:
    terminal_states = {
        'complete',
        'error',
        'removed',
    }

    while True:
        transfer = rpc_call(
            status,
            'aria2.tellStatus',
            [
                gid,
                [
                    'gid',
                    'status',
                    'totalLength',
                    'completedLength',
                    'downloadSpeed',
                    'errorCode',
                    'errorMessage',
                ],
            ],
        )

        print({
            'gid': transfer.get('gid'),
            'status': transfer.get('status'),
            'completed': transfer.get('completedLength'),
            'total': transfer.get('totalLength'),
            'speed': transfer.get('downloadSpeed'),
        })

        if transfer.get('status') in terminal_states:
            return transfer

        time.sleep(interval)


def transfer_worker() -> None:
    status = get_rpc_status(monitored_pid)

    try:
        gid = rpc_call(
            status,
            'aria2.addUri',
            [['https://example.com/file.zip']],
        )

        final_status = monitor_transfer(status, gid)
        print('Final status:', final_status)

    finally:
        status.close()


worker_thread = threading.Thread(
    target=transfer_worker,
    name='aria2-transfer-worker',
    daemon=True,
)

worker_thread.start()
