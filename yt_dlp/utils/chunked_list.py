from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from itertools import islice
import threading
from typing import Iterable, Iterator, overload

@dataclass(slots=True)
class ChunkNode[T]:
    data: tuple[T, ...]
    next: ChunkNode[T] | None = None

class ChunkedList[T]:
    def __init__(self, chunk_size: int = 1000) -> None:
        self.chunk_size: int = chunk_size
        self._head: ChunkNode[T] = ChunkNode(data=())
        self._tail: ChunkNode[T] = self._head
        self._write_lock: threading.Lock = threading.Lock()
        self.clear()

    def chunks(self, *, reverse: bool = False) -> Iterator[tuple[T, ...]]:
        """Exposes a lock-free stream of the raw data chunks.
        
        The reverse argument is strictly keyword-only.
        """
        published_snapshot = self._published_view
        tail_snapshot = self._tail
        chunks = [current.data for _, current in self._iter_chunks(final=tail_snapshot)]

        if published_snapshot:
            chunks.append(published_snapshot)

        if reverse:
            chunks.reverse()

        for chunk in chunks:
            yield chunk

    def compact(self, threshold: float | None = None) -> None:
        """Evaluates the structural density ratio of the container lock-free.
        
        Compacts only if the density drops below the dynamically tracked baseline.
        """
        current_chunks = tuple(self.chunks())
        if not current_chunks:
            return

        full_chunks_count = sum(1 for chunk in current_chunks if len(chunk) == self.chunk_size)
        density_ratio = full_chunks_count / len(current_chunks)

        if threshold is None:
            # Initialized in clear() to 1.0; dynamically tracks best-achieved ratio
            threshold = self._last_compact_ratio

        if density_ratio < threshold:
            self._compact()

    def append(self, item: T) -> None:
        with self._write_lock:
            staging = deque(self._published_view)
            staging.append(item)
            self._published_view = tuple(staging)
            
            if len(self._published_view) >= self.chunk_size:
                self._append_chunk(self._published_view)
                self._reset_active_state()

    def extend(self, values: Iterable[T]) -> None:
        iterator = iter(values)
        with self._write_lock:
            if self._published_view:
                self._append_chunk(self._published_view)
                self._reset_active_state()

            for chunk in iter(lambda: tuple(islice(iterator, self.chunk_size)), ()):
                self._append_chunk(chunk)

    def push(self, item: T) -> None:
        """Alias for append, matching standard push naming mechanics."""
        self.append(item)

    def pop(self) -> T:
        if not self:
            raise IndexError('pop from empty ' + self._class_name())
            
        item = self.__getitem__(-1)
        self.remove(item)
        return item

    def shift(self) -> T:
        if not self:
            raise IndexError('shift from empty ' + self._class_name())
            
        item = self.__getitem__(0)
        self.remove(item)
        return item

    def unshift(self, item: T) -> None:
        with self._write_lock:
            if self._head.next is None:
                staging = deque(self._published_view)
                staging.appendleft(item)
                self._published_view = tuple(staging)
                
                if len(self._published_view) >= self.chunk_size:
                    self._append_chunk(self._published_view)
                    self._reset_active_state()
                return

            new_node = ChunkNode(data=(item,), next=self._head.next)
            self._head.next = new_node
            self._published_chunk_length += 1
        
        self.compact()

    def clear(self) -> None:
        with self._write_lock:
            self._head.next = None
            self._tail = self._head
            self._published_chunk_length = 0
            self._last_compact_ratio = 1.0  
            self._reset_active_state()

    def _compact(self) -> None:
        """Surgically unifies fragmented nodes under the write lock with maximum
        reference retention.
        """
        source_chunks = deque(self.chunks())
        new_head = ChunkNode(data=())
        new_tail = new_head
        new_chunk_len = 0
        buffer: tuple[T, ...] = ()

        with self._write_lock:
            while source_chunks:
                chunk = source_chunks.popleft()

                if len(chunk) == self.chunk_size:
                    if buffer:
                        new_node = ChunkNode(data=buffer, next=None)
                        new_tail.next = new_node
                        new_tail = new_node
                        new_chunk_len += len(buffer)
                        buffer = ()

                    new_node = ChunkNode(data=chunk, next=None)
                    new_tail.next = new_node
                    new_tail = new_node
                    new_chunk_len += self.chunk_size
                    continue

                buffer += chunk
                while len(buffer) >= self.chunk_size:
                    frozen_chunk = buffer[:self.chunk_size]
                    new_node = ChunkNode(data=frozen_chunk, next=None)
                    new_tail.next = new_node
                    new_tail = new_node
                    new_chunk_len += self.chunk_size
                    buffer = buffer[self.chunk_size:]

            self._published_view = buffer
            self._head = new_head
            self._tail = new_tail
            self._published_chunk_length = new_chunk_len

            final_chunks = [current.data for _, current in self._iter_chunks(final=self._tail)]
            if self._published_view:
                final_chunks.append(self._published_view)

            full_count = sum(1 for c in final_chunks if len(c) == self.chunk_size)
            self._last_compact_ratio = full_count / max(1, len(final_chunks))

    def _class_name(self) -> str:
        return self.__class__.__name__

    def _reset_active_state(self) -> None:
        self._published_view: tuple[T, ...] = ()

    def _append_chunk(self, chunk: tuple[T, ...]) -> None:
        new_node = ChunkNode(data=chunk, next=None)
        self._tail.next = new_node
        self._tail = new_node
        self._published_chunk_length += len(chunk)

    def _iter_chunks(self, final: ChunkNode[T] | None = None) -> Iterator[tuple[ChunkNode[T], ChunkNode[T]]]:
        if final is None:
            final = self._tail

        prev: ChunkNode[T] = self._head
        if prev.next is None:
            return

        current: ChunkNode[T] = prev.next
        while current.next is not None:
            yield prev, current
            if current is final:
                return
            prev = current
            current = current.next
        yield prev, current

    def __iter__(self) -> Iterator[T]:
        for chunk in self.chunks():
            for item in chunk:
                yield item

    def __len__(self) -> int:
        published_snapshot = self._published_view
        return self._published_chunk_length + len(published_snapshot)

    def remove(self, value: T) -> bool:
        def _filter_tuple(data_tuple: tuple[T, ...]) -> tuple[T, ...]:
            temp_list = list(data_tuple)
            temp_list.remove(value)
            return tuple(temp_list)

        found = False
        with self._write_lock:
            if value in self._published_view:
                self._published_view = _filter_tuple(self._published_view)
                return True

            for prev, current in self._iter_chunks():
                if value in current.data:
                    new_tuple = _filter_tuple(current.data)

                    new_next = current.next
                    new_tail = prev

                    if new_tuple:
                        new_next = ChunkNode(data=new_tuple, next=current.next)
                        new_tail = new_next

                    if current is self._tail:
                        self._tail = new_tail
                    prev.next = new_next
                    
                    self._published_chunk_length -= 1
                    found = True
                    break

        if found:
            self.compact()

        return found

    def __contains__(self, value: object) -> bool:
        return any(value in chunk for chunk in self.chunks())

    def __reversed__(self) -> Iterator[T]:
        for chunk in self.chunks(reverse=True):
            yield from reversed(chunk)

    @overload
    def __getitem__(self, index: int) -> T: ...
    @overload
    def __getitem__(self, index: slice) -> list[T]: ...

    def __getitem__(self, index: int | slice) -> T | list[T]:
        if isinstance(index, slice):
            return list(self)[index]

        total_len = len(self)
        if index < 0:
            index += total_len

        if index < 0 or index >= total_len:
            raise IndexError(f'{self._class_name()} index out of range')

        running_offset = 0
        for chunk in self.chunks():
            chunk_len = len(chunk)
            if index < running_offset + chunk_len:
                return chunk.__getitem__(index - running_offset)
            running_offset += chunk_len

        raise IndexError(f'{self._class_name()} index out of range')

    def index(self, value: T, start: int = 0, stop: int | None = None) -> int:
        for i, item in enumerate(self):
            if i < start:
                continue
            if stop is not None and i >= stop:
                break
            if item == value:
                return i
        raise ValueError(f'{value} is not in {self._class_name()}')

    def count(self, value: T) -> int:
        return sum(chunk.count(value) for chunk in self.chunks())
