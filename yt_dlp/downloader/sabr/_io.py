from __future__ import annotations
import abc
import io
import os
import queue
import shutil
import threading
import time
import typing


class FormatIOBackend(abc.ABC):
    def __init__(self, fd, filename, buffer=1024 * 1024):
        self.fd = fd
        self.filename = filename
        self.write_buffer = buffer
        self._fp = None
        self._fp_mode = None

    @abc.abstractmethod
    def __len__(self):
        pass

    @property
    def writer(self):
        if self._fp is None or self._fp_mode != 'write':
            return None
        return self._fp

    @property
    def reader(self):
        if self._fp is None or self._fp_mode != 'read':
            return None
        return self._fp

    def initialize_writer(self, resume=False):
        if self._fp is not None:
            raise ValueError('Backend already initialized')

        self._fp = self._create_writer(resume)
        self._fp_mode = 'write'

    @abc.abstractmethod
    def _create_writer(self, resume=False) -> typing.IO:
        pass

    def initialize_reader(self):
        if self._fp is not None:
            raise ValueError('Backend already initialized')
        self._fp = self._create_reader()
        self._fp_mode = 'read'

    @abc.abstractmethod
    def _create_reader(self) -> typing.IO:
        pass

    def close(self):
        if self._fp and not self._fp.closed:
            self._fp.flush()
            self._fp.close()
        self._fp = None
        self._fp_mode = None

    def validate_length(self, expected_length):
        return len(self) == expected_length

    def remove(self):
        self.close()
        self._remove()

    @abc.abstractmethod
    def _remove(self):
        pass

    @abc.abstractmethod
    def exists(self):
        pass

    @property
    def mode(self):
        if self._fp is None:
            return None
        return self._fp_mode

    def write(self, data: io.BufferedIOBase | bytes):
        if not self.writer:
            raise ValueError('Backend writer not initialized')

        if isinstance(data, bytes):
            bytes_written = self.writer.write(data)
        elif isinstance(data, io.BufferedIOBase):
            bytes_written = self.writer.tell()
            shutil.copyfileobj(data, self.writer, length=self.write_buffer)
            bytes_written = self.writer.tell() - bytes_written
        else:
            raise TypeError('Data must be bytes or a BufferedIOBase object')

        self.writer.flush()

        return bytes_written

    def read_into(self, backend):
        if not backend.writer:
            raise ValueError('Destination backend writer not initialized')
        if not self.reader:
            raise ValueError('Backend reader not initialized')
        shutil.copyfileobj(self.reader, backend.writer, length=self.write_buffer)
        backend.writer.flush()


class DiskFormatIOBackend(FormatIOBackend):
    def __len__(self):
        return 0 if not self.exists() else os.path.getsize(self.filename)

    def _create_writer(self, resume=False) -> typing.IO:
        if resume and self.exists():
            write_fp, self.filename = self.fd.sanitize_open(self.filename, 'ab')
        else:
            write_fp, self.filename = self.fd.sanitize_open(self.filename, 'wb')
        return write_fp

    def _create_reader(self) -> typing.IO:
        read_fp, self.filename = self.fd.sanitize_open(self.filename, 'rb')
        return read_fp

    def _remove(self):
        self.fd.try_remove(self.filename)

    def exists(self):
        return os.path.isfile(self.filename)


class MemoryFormatIOBackend(FormatIOBackend):
    def __init__(self, *args, **kwargs):
        self._remove()
        super().__init__(*args, **kwargs)

    def _remove(self):
        self._memory_store = io.BytesIO()

    def _reset(self):
        self._memory_store.seek(0)
        self._memory_store.truncate(0)

    def __len__(self):
        return len(self._memory_store.getvalue())

    def _create_writer(self, resume=False) -> typing.IO:
        class NonClosingBufferedWriter(io.BufferedWriter):
            def close(self):
                self.flush()
                # Do not close the underlying buffer

        if resume and self.exists():
            self._memory_store.seek(0, io.SEEK_END)
        else:
            self._reset()

        return NonClosingBufferedWriter(self._memory_store)

    def _create_reader(self) -> typing.IO:
        class NonClosingBufferedReader(io.BufferedReader):
            def close(self):
                self.flush()

        # Seek to the beginning of the buffer
        self._memory_store.seek(0)
        return NonClosingBufferedReader(self._memory_store)

    def exists(self):
        return len(self) > 0


class ProxiedIOBackend(DiskFormatIOBackend):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._current_mem_be = None
        self._lock = threading.RLock()
        self._worker = None
        self._write_queue = queue.Queue()

        # Initialize the authoritative Disk destination once
        self.initialize_writer(resume=False)

    def _begin_queue(self):
        with self._lock:
            # Start or re-start the worker if it's not active
            if self._worker is None or not self._worker.is_alive():
                base_name = os.path.basename(self.filename)
                self._worker = threading.Thread(
                    target=self._drain_queue,
                    name=f'DiskFormatIOQueue-{base_name}',
                    daemon=True,
                )
                self._worker.start()

    def _create_mem_backend(self):
        """Creates a fresh, memory-only backend instance."""
        mem_backend = MemoryFormatIOBackend(
            fd=self.fd,
            filename=self.filename,
        )
        mem_backend.initialize_writer(resume=False)
        return mem_backend

    def _create_writer(self, resume=False) -> typing.IO:
        disk_write_fp = super()._create_writer(resume)

        class NullWriter:
            def write(self, data): pass
            def flush(self): pass

        with self._lock:
            self._fp_mode = 'write'
            self._fp = disk_write_fp
            if not resume:
                self._fp = NullWriter()
            self._begin_queue()
            if self._current_mem_be:
                self.append(self._current_mem_be)
            if not resume:
                self.flush()
            self._fp = disk_write_fp
            self._current_mem_be = None

        return disk_write_fp

    def _drain_queue(self):
        """
        Worker thread that consumes sealed backends and streams to disk.
        """

        backend = self._write_queue.get()
        while backend is not None:
            try:
                backend.close()
                backend.initialize_reader()
                shutil.copyfileobj(backend.reader, self.writer, length=self.write_buffer)
                self.writer.flush()
            finally:
                # Immediately purge RAM once serialized to disk
                backend.remove()
                self._write_queue.task_done()

            backend = self._write_queue.get()
        else:
            # Poison pill received: Finalize the file handle via parent
            super().close()

    def append(self, backend):
        """Seals the backend and adds it to the queue."""
        backend.close()
        if backend.exists():
            self._write_queue.put(backend)

    def close(self):
        """Stops the worker thread and finalizes the backend."""

        self.flush()
        worker = None
        with self._lock:
            if self._current_mem_be:
                self.append(self._current_mem_be)
            self._current_mem_be = None
            if self._worker is not None and self._worker.is_alive():
                self._write_queue.put(None)
                worker = self._worker
            self._worker = None

        if worker:
            worker.join()

        super().close()

    def flush(self):
        """Blocks until the queue is completely drained to disk."""
        # Ensure any data currently in the active RAM buffer is queued first
        worker = None
        with self._lock:
            # Only join if the worker is actually alive to process it
            if self._worker is not None and self._worker.is_alive():
                worker = self._worker
            if self._current_mem_be:
                self.append(self._current_mem_be)
            self._current_mem_be = None

        if worker:
            # Block until the worker finishes processing all items currently in the queue
            self._write_queue.join()

    def write(self, data: io.BufferedIOBase | bytes):
        """
        Writes to the live memory buffer.
        Queues the buffer only AFTER it crosses the limit.
        """

        with self._lock:
            if self._current_mem_be is None:
                self._current_mem_be = self._create_mem_backend()
            written = self._current_mem_be.write(data)

            if len(self._current_mem_be) > self.write_buffer:
                self.append(self._current_mem_be)
                self._current_mem_be = None

            return written

