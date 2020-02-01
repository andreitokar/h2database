/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

/**
 * A cursor to iterate over elements in ascending order.
 *
 * @param <K> the key type
 * @param <V> the value type
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public final class CursorBuffered<K, V> extends Cursor<K,V> {
    private final KVMapping<K,V>[] buffer;
    private final int bufferLimit;
    private int bufferIndex;
    private K bound;

    CursorBuffered(RootReference<K,V> rootReference, K from, K to, boolean reverse) {
        super(rootReference, from, to, reverse);
        buffer = rootReference.buffer;
        MVMap<K,V> map = rootReference.root.map;
        bufferIndex = reverse ? buffer.length -1 : 0;
        if (from != null) {
            bufferIndex = map.binarySearch(buffer, from, 0);
            if (bufferIndex < 0) {
                bufferIndex = ~bufferIndex;
                if (reverse) {
                    --bufferIndex;
                }
            }
        }
        int bufLimit = reverse ? -1 : buffer.length;
        if (to != null) {
            bufLimit = reverse ?
                    map.binarySearch(buffer, to, 0, bufferIndex - 1) :
                    map.binarySearch(buffer, to, bufferIndex);
            if (bufLimit >= 0) {
                bufLimit += reverse ? -1 : 1;
            } else {
                bufLimit = ~bufLimit;
                if (reverse) {
                    ++bufLimit;
                }
            }
        }
        bufferLimit = bufLimit;
        setupBound();
    }

    @Override
    public boolean hasNext() {
        if (cursorPos != null) {
            int increment = reverse ? -1 : 1;
            while (current == null) {
                Page<K,V> page = cursorPos.page;
                int index = cursorPos.index;
                if (reverse ? index < 0 : index >= upperBound(page)) {
                    // traversal of this page is over, going up a level or stop if at the root already
                    CursorPos<K,V> tmp = cursorPos;
                    if(cursorPos.parent == null) { // we are already at the root level, map is exhausted
                        while (bufferIndex != bufferLimit) {
//                            assert  to == null || Integer.signum(page.map.getKeyType().compare(to, buffer[bufferIndex].key)) == increment;
                            KVMapping<K,V> kvMapping = buffer[bufferIndex];
                            bufferIndex += increment;
                            if (kvMapping.value != null) {
                                current = last = kvMapping.key;
                                lastValue = kvMapping.value;
                                lastPage = page;
                                return true;
                            }
                        }
                        return false;
                    }
                    cursorPos = cursorPos.parent;
                    tmp.parent = keeper;
                    keeper = tmp;
                } else {
                    // traverse down to the leaf taking the leftmost (rightmost for reverse) path
                    while (!page.isLeaf()) {
                        page = page.getChildPage(index);
                        index = reverse ? upperBound(page) - 1 : 0;
                        if (keeper == null) {
                            cursorPos = new CursorPos<>(page, index, cursorPos);
                        } else {
                            CursorPos<K,V> tmp = keeper;
                            keeper = keeper.parent;
                            tmp.parent = cursorPos;
                            tmp.page = page;
                            tmp.index = index;
                            cursorPos = tmp;
                        }
                    }
                    K key = page.getKey(index);
                    V value;
                    int compare = bound == null ? increment : page.map.getKeyType().compare(bound, key);
                    if (reverse ? compare < 0 : compare > 0) {
                        value = page.getValue(index);
                    } else if (bufferIndex != bufferLimit) {
                        KVMapping<K,V> kvMapping = buffer[bufferIndex];
                        bufferIndex += increment;
                        value = kvMapping.value;
                        if (compare != 0) {
                            assert kvMapping.index < 0;
                            assert kvMapping.value != null;
                            key = bound;
                            cursorPos.index -= increment;
                        } else {
                            assert kvMapping.index >= 0 : kvMapping + " " + index + " " + key + " " + bound;
                            assert kvMapping.index == index : kvMapping + " " + index;
                            if (value == null) {
                                key = null;
                            }
                        }
                        setupBound();
                    } else if (compare == 0) {
                        value = page.getValue(index);
                    } else {
                        return false;
                    }
                    current = last = key;
                    lastValue = value;
                    lastPage = page;
                }
                cursorPos.index += increment;
            }
        }
        return current != null;
    }

    private void setupBound() {
        bound = to;
        if (bufferIndex != bufferLimit) {
            K boundKey = buffer[bufferIndex].key;
            if (to == null || Integer.signum(lastPage.map.getKeyType().compare(boundKey, to)) != (reverse ? -1 : 1)) {
                bound = boundKey;
            }
        }
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
            assert cursorPos != null;
            CursorPos<K,V> cp = cursorPos;
            CursorPos<K,V> parent;
            while ((parent = cp.parent) != null) cp = parent;
            Page<K,V> root = cp.page;
            MVMap<K,V> map = root.map;
            long index = map.getKeyIndex(next());
            last = map.getKey(index + (reverse ? -n : n));
            this.cursorPos = traverseDown(root, last, reverse);
        }
    }
}
