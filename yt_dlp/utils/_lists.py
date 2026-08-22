import collections.abc
import itertools


class ChunkedList(collections.abc.Sequence):

    class IndexError(IndexError):  # noqa: A001
        pass

    def __init__(self, iterable=None, *, chunk_size=None):
        if chunk_size is None:
            chunk_size = 1 << 13
        self.chunk_size = chunk_size
        self._chunks = []
        self._length = 0
        if iterable is not None:
            self.extend(iterable)

    def append(self, item):
        # Create a new partial chunk only if the cache is empty or the last chunk is full
        if not self._chunks or len(self._chunks[-1]) >= self.chunk_size:
            self._chunks.append([item])
        else:
            self._chunks[-1].append(item)
        self._length += 1

    def extend(self, iterable):
        iterator = iter(iterable)

        # If chunks exist and the last one has space, fill it completely first
        if self._chunks and len(self._chunks[-1]) < self.chunk_size:
            space_left = self.chunk_size - len(self._chunks[-1])
            initial_slice = list(itertools.islice(iterator, space_left))

            if not initial_slice:
                return

            self._chunks[-1].extend(initial_slice)
            self._length += len(initial_slice)

        # Pull remaining chunks from the source iterable.
        # This will strictly yield fully filled chunks, or a single partially filled final chunk.
        chunk_fetcher = iter(lambda: list(itertools.islice(iterator, self.chunk_size)), [])
        for chunk in chunk_fetcher:
            self._chunks.append(chunk)
            self._length += len(chunk)

    def __len__(self):
        return self._length

    def __iter__(self):
        for chunk in self._chunks:
            yield from chunk

    def __reversed__(self):
        for chunk in reversed(self._chunks):
            yield from reversed(chunk)

    def __contains__(self, value):
        return any(value in chunk for chunk in self._chunks)

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return super().__eq__(other)
        if self.chunk_size != other.chunk_size or len(self) != len(other):
            return False
        return self._chunks == other._chunks

    def __getitem__(self, idx):
        if isinstance(idx, slice):
            start, stop, step = idx.indices(self._length)

            start_chunk = start // self.chunk_size
            start_offset = start % self.chunk_size

            stop_chunk = stop // self.chunk_size
            stop_offset = stop % self.chunk_size

            if start_chunk == stop_chunk or (start < stop and 1 == step):
                cl = type(self)(chunk_size=self.chunk_size)
                try:
                    if start_chunk == stop_chunk:
                        cl.extend(self._chunks[start_chunk][start_offset:stop_offset:step])
                    else:
                        cl.extend(self._chunks[start_chunk][start_offset:])
                        for c_idx in range(1 + start_chunk, stop_chunk):
                            cl.extend(self._chunks[c_idx])
                        if stop_offset > 0:
                            cl.extend(self._chunks[stop_chunk][:stop_offset])
                except IndexError as e:
                    raise self.IndexError(e) from e
                else:
                    return cl

            return type(self)((self._get_element(i) for i in range(start, stop, step)), chunk_size=self.chunk_size)
        elif isinstance(idx, int):
            return self._get_element(idx)
        else:
            raise TypeError('indices must be integers or slices')

    def _get_element(self, idx):
        if idx < 0:
            idx += self._length
        if not (0 <= idx < self._length):
            raise self.IndexError('index out of range')
        try:
            return self._chunks[idx // self.chunk_size][idx % self.chunk_size]
        except IndexError as e:
            raise self.IndexError(e) from e


class LazyList(collections.abc.Sequence):
    """Lazy immutable list from an iterable
    Note that slices of a LazyList are lists and not LazyList"""

    class IndexError(IndexError):  # noqa: A001
        pass

    def __init__(self, iterable, *, reverse=False):
        self._reversed = reverse
        self._is_self = isinstance(iterable, type(self))
        if self._is_self:
            self._iterable = iterable._iterable
            self._cache = iterable._cache
        else:
            self._iterable = iter(iterable)
            self._cache = ChunkedList()

    def __iter__(self):
        if self._reversed:
            # We need to consume the entire iterable to iterate in reverse
            yield from reversed(self._exhaust())
            return

        def populate_cache():
            cache_position = len(self._cache)
            for item in self._iterable:
                self._cache.append(item)
                # catch-up to additional items from the cache
                for i in range(cache_position, len(self._cache) - 1):
                    cache_position = 1 + i
                    yield self._cache[i]
                cache_position += 1
                yield item
            for i in range(cache_position, len(self._cache)):
                cache_position = 1 + i
                yield self._cache[i]

        yield from itertools.chain(self._cache, populate_cache())

    def _exhaust(self):
        self._cache.extend(self._iterable)
        self._iterable = []  # Discard the emptied iterable to make it pickle-able
        return self._cache

    def exhaust(self):
        """Evaluate the entire iterable"""
        # guarantee a list is returned
        l = list(self._exhaust())
        if self._reversed:
            l.reverse()
        return l

    @staticmethod
    def _reverse_index(x):
        return None if x is None else ~x

    def __getitem__(self, idx):
        if isinstance(idx, slice):
            if self._reversed:
                idx = slice(self._reverse_index(idx.start), self._reverse_index(idx.stop), -(idx.step or 1))
            start, stop, step = idx.start, idx.stop, idx.step or 1
        elif isinstance(idx, int):
            if self._reversed:
                idx = self._reverse_index(idx)
            start, stop, step = idx, idx, 0
        else:
            raise TypeError('indices must be integers or slices')
        if ((start or 0) < 0 or (stop or 0) < 0
                or (start is None and step < 0)
                or (stop is None and step > 0)):
            # We need to consume the entire iterable to be able to slice from the end
            # Obviously, never use this with infinite iterables
            self._exhaust()
        else:
            n = 1 + max(start or 0, stop or 0) - len(self._cache)
            if n > 0:
                self._cache.extend(itertools.islice(self._iterable, n))
        try:
            r = self._cache[idx]
            # guarantee a list is returned for slices
            return list(r) if step else r
        except IndexError as e:
            raise self.IndexError(e) from e

    def __bool__(self):
        try:
            self[-1 if self._reversed else 0]
        except self.IndexError:
            return False
        return True

    def __len__(self):
        return len(self._exhaust())

    def __reversed__(self):
        return type(self)(self, reverse=not self._reversed)

    def __copy__(self):
        return type(self)(self, reverse=self._reversed)

    def __repr__(self):
        # repr and str should mimic a list. So we exhaust the iterable
        return repr(self.exhaust())

    def __str__(self):
        return repr(self.exhaust())


class PagedList:

    class IndexError(IndexError):  # noqa: A001
        pass

    def __len__(self):
        # This is only useful for tests
        return len(self.getslice())

    def __init__(self, pagefunc, pagesize, use_cache=True):
        self._pagefunc = pagefunc
        self._pagesize = pagesize
        self._pagecount = float('inf')
        self._use_cache = use_cache
        self._cache = {}

    def getpage(self, pagenum):
        page_results = self._cache.get(pagenum)
        if page_results is None:
            page_results = [] if pagenum > self._pagecount else list(self._pagefunc(pagenum))
        if self._use_cache:
            self._cache[pagenum] = page_results
        return page_results

    def getslice(self, start=0, end=None):
        return list(self._getslice(start, end))

    def _getslice(self, start, end):
        raise NotImplementedError('This method must be implemented by subclasses')

    def __getitem__(self, idx):
        assert self._use_cache, 'Indexing PagedList requires cache'
        if not isinstance(idx, int) or idx < 0:
            raise TypeError('indices must be non-negative integers')
        entries = self.getslice(idx, 1 + idx)
        if not entries:
            raise self.IndexError
        return entries[0]

    def __bool__(self):
        return bool(self.getslice(0, 1))


class OnDemandPagedList(PagedList):
    """Download pages until a page with less than maximum results"""

    def _getslice(self, start, end):
        for pagenum in itertools.count(start // self._pagesize):
            firstid = pagenum * self._pagesize
            nextfirstid = firstid + self._pagesize
            if start >= nextfirstid:
                continue

            startv = (
                start % self._pagesize
                if firstid <= start < nextfirstid
                else 0)
            endv = (
                1 + ((end - 1) % self._pagesize)
                if (end is not None and firstid <= end <= nextfirstid)
                else None)

            try:
                page_results = self.getpage(pagenum)
            except Exception:
                self._pagecount = pagenum - 1
                raise
            if startv != 0 or endv is not None:
                page_results = page_results[startv:endv]
            yield from page_results

            # A little optimization - if current page is not "full", ie. does
            # not contain page_size videos then we can assume that this page
            # is the last one - there are no more ids on further pages -
            # i.e. no need to query again.
            if len(page_results) + startv < self._pagesize:
                break

            # If we got the whole page, but the next page is not interesting,
            # break out early as well
            if end == nextfirstid:
                break


class InAdvancePagedList(PagedList):
    """PagedList with total number of pages known in advance"""

    def __init__(self, pagefunc, pagecount, pagesize):
        super().__init__(pagefunc, pagesize, True)
        self._pagecount = pagecount

    def _getslice(self, start, end):
        start_page = start // self._pagesize
        end_page = self._pagecount if end is None else min(self._pagecount, 1 + end // self._pagesize)
        skip_elems = start - start_page * self._pagesize
        only_more = None if end is None else end - start
        for pagenum in range(start_page, end_page):
            page_results = self.getpage(pagenum)
            if skip_elems:
                page_results = page_results[skip_elems:]
                skip_elems = None
            if only_more is not None:
                prl = len(page_results)
                if prl < only_more:
                    only_more -= prl
                else:
                    yield from page_results[:only_more]
                    break
            yield from page_results
