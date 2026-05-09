from __future__ import annotations

import collections
import dataclasses
import hashlib
from pathlib import Path
from yt_dlp.utils import DownloadError
from ._io import DiskFormatIOBackend, MemoryFormatIOBackend, ProxiedIOBackend


@dataclasses.dataclass
class Segment:
    segment_id: str
    content_length: int | None = None
    content_length_estimated: bool = False
    sequence_number: int | None = None
    start_time_ms: int | None = None
    duration_ms: int | None = None
    duration_estimated: bool = False
    is_init_segment: bool = False


@dataclasses.dataclass
class Sequence:
    sequence_id: str
    # The segments may not have a start byte range, so to keep it simple we will track
    # length of the sequence. We can infer from this and the segment's content_length where they should end and begin.
    sequence_content_length: int = 0
    first_segment: Segment | None = None
    last_segment: Segment | None = None


@dataclasses.dataclass
class RecoveryPackage:
    backend: MemoryFormatIOBackend
    pre_checksum: str
    post_checksum: str
    offset: int
    length: int


class SequenceFile:

    def __init__(self, fd, format_filename, sequence: Sequence, resume=False, max_segments=None, segment_memory_file_limit=None):
        self.fd = fd
        self.format_filename = format_filename
        self.sequence = sequence
        self.file = DiskFormatIOBackend(
            fd=self.fd,
            filename=self.format_filename + f'.sq{self.sequence_id}.part',
        )
        self.current_segment: SegmentFile | None = None
        self.resume = resume
        self.max_segments = max_segments
        self.segment_memory_file_limit = segment_memory_file_limit

        sequence_file_exists = self.file.exists()

        if not resume and sequence_file_exists:
            self.file.remove()

        elif not self.sequence.last_segment and sequence_file_exists:
            self.file.remove()

        if self.sequence.last_segment and not sequence_file_exists:
            raise DownloadError(f'Cannot find existing sequence {self.sequence_id} file')

        if self.sequence.last_segment and not self.file.validate_length(self.sequence.sequence_content_length):
            self.file.remove()
            raise DownloadError(f'Existing sequence {self.sequence_id} file is not valid; removing')

    @property
    def sequence_id(self):
        return self.sequence.sequence_id

    @property
    def current_length(self):
        total = self.sequence.sequence_content_length
        if self.current_segment:
            total += self.current_segment.current_length
        return total

    def is_next_segment(self, segment: Segment):
        if self.current_segment:
            return False
        previous_segment = self.sequence.last_segment or self.sequence.first_segment
        if not previous_segment:
            return True
        if previous_segment.is_init_segment:
            # Currently we only allow init segments in their own sequence
            return False
        if (
            self.max_segments
            and (self.sequence.last_segment.sequence_number - (self.sequence.first_segment.sequence_number - 1)) >= self.max_segments
        ):
            return False

        return segment.sequence_number == previous_segment.sequence_number + 1

    def is_current_segment(self, segment_id: str):
        if not self.current_segment:
            return False
        return self.current_segment.segment_id == segment_id

    def initialize_segment(self, segment: Segment):
        if not self.current_segment and not self.is_next_segment(segment):
            raise ValueError('Cannot initialize a segment that does not match the next segment')

        if self.current_segment:
            if not self.is_current_segment(segment.segment_id):
                raise ValueError('Cannot reinitialize a segment that does not match the current segment')
            # Segment re-initialization: ensure previous segment is closed.
            # Windows does not allow writing to an open file.
            self.current_segment.close()

        self.current_segment = SegmentFile(
            fd=self.fd,
            format_filename=self.format_filename,
            segment=segment,
            memory_file_limit=self.segment_memory_file_limit,
        )

    def write_segment_data(self, data, segment_id: str):
        if not self.is_current_segment(segment_id):
            raise ValueError('Cannot write to a segment that does not match the current segment')

        self.current_segment.write(data)

    def end_segment(self, segment_id):
        if not self.is_current_segment(segment_id):
            raise ValueError('Cannot end a segment that does not exist')

        self.current_segment.finish_write()

        if (
            self.current_segment.segment.content_length
            and not self.current_segment.segment.content_length_estimated
            and self.current_segment.current_length != self.current_segment.segment.content_length
        ):
            raise DownloadError(
                f'Filesize mismatch for segment {self.current_segment.segment_id}: '
                f'Expected {self.current_segment.segment.content_length} bytes, got {self.current_segment.current_length} bytes')

        self.current_segment.segment.content_length = self.current_segment.current_length
        self.current_segment.segment.content_length_estimated = False

        if not self.sequence.first_segment:
            self.sequence.first_segment = self.current_segment.segment

        self.sequence.last_segment = self.current_segment.segment
        self.sequence.sequence_content_length += self.current_segment.current_length

        if not self.file.mode:
            self.file.initialize_writer(self.resume)

        # Segment file may not exist if no data was written
        if self.current_segment.exists():
            self.current_segment.read_into(self.file)

        self.current_segment.remove()
        self.current_segment = None

    def read_into(self, backend):
        self.file.initialize_reader()
        self.file.read_into(backend)
        self.file.close()

    def remove(self):
        self.close()
        self.file.remove()
        if self.current_segment:
            self.current_segment.remove()
            self.current_segment = None

    def close(self):
        self.file.close()
        if self.current_segment:
            self.current_segment.close()


class SegmentFile:

    def __init__(self, fd, format_filename, segment: Segment, memory_file_limit=None):
        self.fd = fd
        self.format_filename = format_filename
        self.segment: Segment = segment
        self._cumulative_hasher = hashlib.sha256()
        self._diverted_packages = list()
        self._expected_position = 0
        self._is_diverted = False
        self._known_good_position = 0
        # Initialize to the hash of an empty stream to start the chain
        self._known_good_checksum = self._cumulative_hasher.hexdigest()
        self._packages = collections.deque(maxlen=8)

        if memory_file_limit is None:
            self.memory_file_limit = 2 * 1024 * 1024  # Default to 2 MB
        else:
            self.memory_file_limit = memory_file_limit

        filename = format_filename + f'.sg{segment.segment_id}.part'
        # Store the segment in memory first
        # After writing more than the limit, then promote it to disk
        self.file = MemoryFormatIOBackend(
            fd=self.fd,
            filename=filename,
        )

        # Never resume a segment
        # Remove an existing promoted file first
        # Later when the limit was exceeded,
        # the disk backend would remove the file for us.
        # Since the memory backend won't clear files,
        # handle this ourselves here.
        disk_file = Path(self.file.filename)
        if disk_file.is_file():
            disk_file.unlink()

    @property
    def current_length(self):
        """Live size reported by the backend."""
        disk_size = len(self.file)
        if isinstance(self.file, ProxiedIOBackend):
            self.file.flush()
            fp = Path(self.file.filename)
            disk_size = fp.stat().st_size if fp.is_file() else 0
        if not self._is_diverted:
            return disk_size
        return self._known_good_position + sum(p.length for p in self._diverted_packages)

    @property
    def segment_id(self):
        return self.segment.segment_id

    def write(self, data):
        package = self._create_package(data)

        if self._is_diverted:
            self._diverted_packages.append(package)
            return

        self._packages.append(package)

        try:
            if not self.file.mode:
                self.file.initialize_writer(resume=False)

            # Use append() when available
            if hasattr(self.file, 'append') and callable(self.file.append):
                self.file.append(package.backend)
            else:
                package.backend.initialize_reader()
                self.file.write(package.backend.reader)
                package.backend.close()
                mem_backend_too_large = (
                    isinstance(self.file, MemoryFormatIOBackend)
                    and len(self.file) > self.memory_file_limit
                )
                if mem_backend_too_large:
                    self._promote_to_disk()
        except (OSError, DownloadError):
            self._is_diverted = True
            self.file.close()
            self.file.initialize_reader()
            disk_size = len(self.file)
            self.file.close()
            previous_pkg = None
            for pkg in iter(self._packages):
                if (pkg.offset + pkg.length) < disk_size:
                    self._known_good_checksum = pkg.pre_checksum
                    self._known_good_position = pkg.offset
                    previous_pkg = pkg
                else:
                    if previous_pkg is not None:
                        self._diverted_packages.append(previous_pkg)
                        previous_pkg = None
                    self._diverted_packages.append(pkg)
        else:
            # Update the known good state only on successful write
            self._known_good_position = self._expected_position
            self._known_good_checksum = package.post_checksum

    def read_into(self, destination):
        hasher = hashlib.sha256()
        # Read the verified portion of the disk file
        if self.file.exists():
            self.file.close()
            self._read_up_to(hasher.update, self._known_good_position)
            if hasher.hexdigest() != self._known_good_checksum:
                raise DownloadError(f'Disk corruption in segment {self.segment.segment_id}')
            for pkg in self._diverted_packages:
                # Pre-condition: Prove end of disk matches start of memory chain
                if hasher.hexdigest() != pkg.pre_checksum:
                    raise DownloadError(f'Integrity gap detected before package at offset {pkg.offset}')
                pkg.backend.initialize_reader()
                hasher.update(pkg.backend.reader.read())
                pkg.backend.close()
            if hasher.hexdigest() != self._cumulative_hasher.hexdigest():
                raise DownloadError(f'Final integrity check failed for segment {self.segment.segment_id}')
            # Logically truncate: stop exactly at the last known good byte
            self._read_up_to(destination.writer.write, self._known_good_position, length=destination.write_buffer)

        # Append all diverted packages
        for pkg in self._diverted_packages:
            pkg.backend.initialize_reader()
            pkg.backend.read_into(destination)
            pkg.backend.close()

    def exists(self):
        return self.file.exists()

    def remove(self):
        self.close()
        self.file.remove()
        for pkg in self._diverted_packages:
            pkg.backend.remove()
        self._diverted_packages.clear()
        self._packages.clear()

    def finish_write(self):
        self.close()

    def close(self):
        self.file.close()

    def _create_package(self, data):
        # Create memory-backed package
        pre_checksum = self._cumulative_hasher.hexdigest()
        start_offset = self._expected_position
        # Named specifically for the offset and cleanup pattern
        pkg_filename = f'{self.format_filename}.sg{self.segment_id}.pkg.{start_offset}.part'
        pkg_backend = MemoryFormatIOBackend(self.fd, pkg_filename)
        pkg_backend.initialize_writer()
        pkg_backend.write(data)
        pkg_backend.close()

        data_bytes = pkg_backend._memory_store.getvalue()
        assert isinstance(data_bytes, bytes), type(data_bytes)

        # Update running state
        self._cumulative_hasher.update(data_bytes)
        self._expected_position += len(data_bytes)
        post_checksum = self._cumulative_hasher.hexdigest()

        return RecoveryPackage(
            backend=pkg_backend,
            pre_checksum=pre_checksum,
            post_checksum=post_checksum,
            offset=start_offset,
            length=len(data_bytes),
        )

    def _promote_to_disk(self):
        old_mem_backend = self.file
        new_disk_backend = ProxiedIOBackend(
            fd=old_mem_backend.fd,
            filename=old_mem_backend.filename,
        )

        try:
            new_disk_backend.remove()
            new_disk_backend.initialize_writer(resume=False)
            new_disk_backend.append(old_mem_backend)
        except Exception:
            old_mem_backend.close()
            old_mem_backend.initialize_writer(resume=True)
            new_disk_backend.remove()
            raise
        else:
            self.file = new_disk_backend

    def _read_up_to(self, func, /, limit=0, *, length=None):
        assert callable(func), type(func)
        if length is None:
            # default to a size that fits in a CPU cache
            length = 1024 * 32  # KiB

        self.file.initialize_reader()
        remaining = limit
        while remaining > 0:
            cs = min(remaining, length)
            b = self.file.reader.read(cs)
            if not b:
                break
            func(b)
            remaining -= len(b)
        self.file.close()
