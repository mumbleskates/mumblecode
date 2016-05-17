# mumblecode
A collection of useful tools I have developed over time in Python to ease my own development. A work in progress.

## caching
Provides very basic persistent key-value stores for Requests responses and API caching.

## collections
Some specialized collections tools that I could not find implemented with good time complexity elsewhere (see: jaraco.collections.RangeMap, a comprehensive solution with lamentable time complexity), largely revolving around interval based queries.

* LeastValue, GreatestValue, JustBefore, JustAfter: Specially-sorting value objects
* interval tree intersections, merges, and overlap functions in best-time, made for use with the excellent intervaltree module
* Sample, SampledValue: designed for storing and querying values sampled along a continuum in best-time. For really huge datasets this should be changed to utilize something like blist for its underlying storage
* IntervalMapping: a mapping of non-overlapping half-open intervals, queryable in best-time. For very large datasets and constant modification, like SampledValue, this should be patched with an underlying list with better asymptotic performance
* HistorySet, HistoryDict, now(): drop-in replacements for set and dict that remember everything that happens to them and when with current timestamps; underneath they are effectively append-only, and can be queried for their complete state at any timestamp. Any other sortable type can also be used in place of the time value

## context
Provides `reentrant`, a context manager wrapper class that protects its underlying context manager from reentrant usage. For example, sqlite3.Connection will effectively ignore entrances to context and will COMMIT on context exit. Wrapping in this class will then prevent the commit from occurring until all the contexts have been exited.

## decorate
Decorator tools; currently contains a 2.7-portable exception suppressor.

## graphs
Graph traversal and complex weighting.

* dijkstra, dijkstra_first, etc.: generalized dijkstra implementation that accepts multiple starting points, a predicate for an acceptable ending point, and an edge-finder function and returns a generator that will yield complete paths from starting points to destinations from shortest to longest
* SumTuple: a tuple subclass that sums respective elements upon addition rather than concatenating
* MaxFirstSumTuple: like the above, but only takes the maximum of the first element (allowing its use as an overriding path preference weight)
* Worker, WorkPerformed: allows summed traversal weights to carry unique implementations of graph weight calculation with them, so multiple workers that may traverse the graph very differently can be deployed from different starting points

## iterables
Two tools for merging multiple iterators of sorted values into a single resulting stream by slightly differing semantics.

## multithreading
Tools for multithreaded implementations with workers.

* CloseableQueue: a subclass of queue.Queue which can be closed when all jobs or results have been inserted, alleviating the need to signal the end of the thread to consumers in other ways
* IterProvider: iterable that multiplexes any thread-local iterator from a dedicated worker thread to any number of consumers. The worker thread will shut down cleanly even if the resulting iterator is not fully consumed before it is discarded
* QueryProvider: a subclass of the above for sqlite3 cursor results

## ratelimiting
A very simple implementation of a rate limiter to prevent API spam in a highly concurrent scraper.

## text
Text tools.

* check_parens: flexible brace matching checks