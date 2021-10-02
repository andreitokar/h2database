/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import org.h2.mvstore.type.DataType;

/**
 * A cursor to iterate over elements in ascending order.
 *
 * @param <K> the key type
 * @param <V> the value type
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public final class CursorBuffered<K, V> extends Cursor<K,V>
{
    private static final int LEVELS = 16;

    private final DataType<K> comparator;
    @SuppressWarnings("unchecked")
    private final KVMapping<K,V>[][] buffers = (KVMapping<K,V>[][])new KVMapping[LEVELS][];
    private final int[] bufferLimits = new int[LEVELS];
    private final int[] bufferIndexes = new int[LEVELS];
    @SuppressWarnings("unchecked")
    private final K[] keys = (K[])new Object[LEVELS];
    private final int[] heap = new int [LEVELS];
    private       int level;
    private       int buffersCount;
    private       Page<K,V> coverPage;

    CursorBuffered(RootReference<K,V> rootReference, K from, K to, boolean reverse) {
        super(rootReference, from, to, reverse);
        assert cursorPos != null;
        comparator = lastPage.map.getKeyType();

        init(from, to, reverse);
    }

    private void init(K from, K to, boolean reverse) {
        cursorPos = CursorPos.reverse(cursorPos, null);
        for (CursorPos<K,V> cp = cursorPos; cp != null; cp = cp.parent, ++level) {
            Page<K, V> page = cp.page;
            KVMapping<K, V>[] buffer = page.getBuffer();
            if (buffer != null) {
                addBufferToHeap(buffer, from, coverPage == null ? to : null, reverse, level);
            }
            cp.limit = to != null ? page.normalizeIndex(page.binarySearch(to), !reverse) :
                       reverse ? 0 : upperBound(page);
            if (coverPage == null && (page.isLeaf() || cp.index != cp.limit)) {
                coverPage = page;
            }
        }
        --level;
        cursorPos = CursorPos.reverse(cursorPos, null);
    }

    private void addBufferToHeap(KVMapping<K, V>[] buffer, K from, K to, boolean reverse, int slot) {
        MVMap<K, V> map = lastPage.map;

        int current;
        if (from == null) {
            current = reverse ? buffer.length - 1 : 0;
        } else {
            current = normalizeIndex(map.binarySearch(buffer, from, 0), reverse);
        }

        int bufLimit;
        if (to == null) {
            bufLimit = reverse ? 0 : buffer.length - 1;
        } else {
            bufLimit = reverse ?
                    map.binarySearch(buffer, to, 0, current):
                    map.binarySearch(buffer, to, current);
            bufLimit = normalizeIndex(bufLimit, !reverse);
        }

        if (reverse ? current >= bufLimit : current <= bufLimit) {
            buffers[slot] = buffer;
            bufferIndexes[slot] = current;
            bufferLimits[slot] = bufLimit;
            keys[slot] = currentKey(slot);
            assert validateBuffer(to, reverse, slot, current, bufLimit);
            heap[buffersCount] = slot;
            siftUp(buffersCount++);
        }
    }

    private boolean validateBuffer(K to, boolean reverse, int slot, int current, int bufLimit) {
        assert to == null || Integer.signum(comparator.compare(keys[slot], to)) != (reverse ? -1 : 1);
        if (to != null) {
            for (int i = current; i <= bufLimit; i++) {
                assert Integer.signum(comparator.compare(buffers[slot][i].key, to)) != (reverse ? -1 : 1);
            }
        }
        return true;
    }

    @Override
    public boolean hasNext() {
        assert validateHeap();
        if (cursorPos != null) {
            int increment = reverse ? -1 : 1;
            while (current == null) {
                Page<K,V> page = cursorPos.page;
                int index = cursorPos.index;
                if (reverse ? index < cursorPos.limit : index > cursorPos.limit) {
                    // traversal of this page is over, going up a level or stop if it was the root already
                    KVMapping<K, V>[] buffer = page.getBuffer();
                    if (buffer != null) {
                        // pull out any entries from this page's buffer,
                        // buffer's key range may extend beyond page itself
                        while (buffer == buffers[level]) {
                            KVMapping<K, V> kvMapping = pullOut();
                            if (kvMapping.value != null) {
                                current = last = kvMapping.key;
                                lastValue = kvMapping.value;
                                lastPage = page;
                                assert to == null || Integer.signum(comparator.compare(last, to)) != increment;
                                assert validateHeap();
                                return true;
                            }
                        }
                    }
                    // and if there isn't any now, proceed to a parent page
                    --level;
                    CursorPos<K, V> tmp = cursorPos;
                    cursorPos = cursorPos.parent;
                    if (cursorPos == null) { // we are already at the root level, map is exhausted
                        return false;
                    }
                    tmp.parent = keeper;
                    keeper = tmp;
                } else if (page.isLeaf()) {
                    K key = page.getKey(index);
                    int compare = buffersCount == 0 ? increment : Integer.signum(comparator.compare(keys[heap[0]], key));

                    V value;
                    if (compare == increment) {
                        // take mapping from the page
                        value = page.getValue(index);
                        // to ensure advancements within a page
                        compare = 0;
                    } else {    // take mapping from the heap (buffers)
                        KVMapping<K,V> kvMapping = pullOut();
                        assert kvMapping != null;
                        key = kvMapping.key;
                        value = kvMapping.value;
                    }
                    if (value != null) {
                        current = last = key;
                        lastValue = value;
                        lastPage = page;
                    }
                    // page index only need to be advanced
                    // if mapping was taken from page only
                    // or both keys (on a page and in the buffer) are the same
                    if (compare != 0) {
                        continue;
                    }
                } else {
                    // traverse down to the leaf taking the leftmost (rightmost for reverse) path
                    while (!page.isLeaf()) {
                        boolean atStop = page == coverPage && index == cursorPos.limit;
                        page = page.getChildPage(index);
                        index = reverse ? upperBound(page) : 0;
                        ++level;
                        KVMapping<K, V>[] buffer = page.getBuffer();
                        if (buffer != null) {
                            addBufferToHeap(buffer, null, atStop ? to : null, reverse, level);
                        }
                        allocateCursorPos(page, index);
                        if (atStop && to != null) {
                            coverPage = page;
                            cursorPos.limit = coverPage.normalizeIndex(coverPage.binarySearch(to), !reverse);
                        } else {
                            cursorPos.limit = reverse ? 0 : upperBound(cursorPos.page);
                        }
                    }
                    continue;
                }
                cursorPos.index += increment;
            }
        }
        assert validateHeap();
        return current != null;
    }
    private KVMapping<K,V> pullOut() {
        assert buffersCount != 0;
        assert validateHeap();
        KVMapping<K,V> kvMapping = currentMapping(heap[0]);
        do {
            if (advance()) {
                heap[0] = heap[--buffersCount];
                if (buffersCount == 0) {
                    break;
                }
            }
            siftDown();
            // need to pull all instances of the given key out of the heap
        } while(comparator.compare(kvMapping.key, keys[heap[0]]) == 0);
        assert validateHeap();
        return kvMapping;
    }

    private boolean advance() {
        int indx = 0;
        assert validateHeap();
        int slot = heap[indx];
        boolean exhausted = bufferIndexes[slot] == bufferLimits[slot];
        if (exhausted) {
            buffers[slot] = null;
        } else {
            bufferIndexes[slot] += reverse ? -1 : 1;
            keys[slot] = currentKey(slot);
            assert to == null || Integer.signum(comparator.compare(keys[slot], to)) != (reverse ? -1 : 1);
        }
        return exhausted;
    }

    private void siftUp(int child) {
        int tmp = heap[child];
        for(int parent; child > 0 && compare(heap[parent = (child-1) >>> 1], tmp) > 0; child = parent) {
            heap[child] = heap[parent];
        }
        heap[child] = tmp;
    }

    private void siftDown() {
        int parent = 0;
        int tmp = heap[parent];
        int half = buffersCount >>> 1;
        while (parent < half) {
            int child = (parent << 1) + 1;
            int c = heap[child];
            int right = child + 1;
            if (right < buffersCount && compare(c, heap[right]) > 0) {
                c = heap[child = right];
            }
            if (compare(tmp, c) <= 0) {
                break;
            }
            heap[parent] = c;
            parent = child;
        }
        heap[parent] = tmp;
    }

    private boolean validateHeap() {
   		for(int child = 1, parent = 0; child < buffersCount; parent += (++child & 1)) {
   		    assert compare(heap[parent], heap[child]) < 0;
   		}
   		return true;
   	}

   	private int compare(int one, int two) {
        int res = comparator.compare(keys[one], keys[two]);
        if (res == 0) {
            return Integer.compare(one, two);
        }
        return reverse ? -res : res;
    }

    private K currentKey(int index) {
        return currentMapping(index).key;
    }

    private KVMapping<K, V> currentMapping(int slot) {
        return buffers[slot][bufferIndexes[slot]];
    }

    /**
     * Skip over that many entries. This method is relatively fast (for this map
     * implementation) even if many entries need to be skipped.
     *
     * @param n the number of entries to skip
     */
    public void skip(long n) {
        if (n < 10) {
            while (n-- > 0 && hasNext()) {
                next();
            }
        } else if(hasNext()) {
            // TODO: optimize this to jump over nodes instead
            assert cursorPos != null;
            CursorPos<K,V> cp = cursorPos;
            CursorPos<K,V> parent;
            while ((parent = cp.parent) != null) cp = parent;
            Page<K,V> root = cp.page;
            MVMap<K,V> map = root.map;
            long index = map.getKeyIndex(next());
            last = map.getKey(index + (reverse ? -n : n));
            cursorPos = traverseDown(root, last, reverse);
            buffersCount = 0;
            level = 0;
            init(last, to, reverse);
        }
    }
}
