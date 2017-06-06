/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.util.Iterator;

/**
 * A cursor to iterate over elements in ascending order.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public final class Cursor<K, V> implements Iterator<K> {

    private final MVMap<K, ?> map;
    private final boolean snapshot;
    private CursorPos cursorPos;
    private K anchor;
    private K current, last;
    private V currentValue;
    private Page lastPage;

    Cursor(MVMap<K, ?> map, Page root, K from) {
        this(map, root, from, true);
    }

    public Cursor(MVMap<K, ?> map, Page root, K from, boolean snapshot) {
        this.map = map;
        this.snapshot = snapshot;
        this.anchor = from;
        this.cursorPos = traverseDown(root, from);
    }

    @Override
    public boolean hasNext() {
        if(current == null) {
            fetchNext();
        }
        return current != null;
    }

    @Override
    public K next() {
        if(!hasNext()) {
            return null;
        }
        anchor = current;
        last = current;
        current = null;
        return anchor;
    }

    /**
     * Get the last read key if there was one.
     *
     * @return the key or null
     */
    public K getKey() {
        return current;
    }

    /**
     * Get the last read value if there was one.
     *
     * @return the value or null
     */
    public V getValue() {
        return currentValue;
    }

    Page getPage() {
        return lastPage;
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
            long index = map.getKeyIndex(next());
            anchor = map.getKey(index + n);
            cursorPos = traverseDown(map.getRootPage(), anchor);
        }
    }

    @Override
    public void remove() {
        throw DataUtils.newUnsupportedOperationException(
                "Removing is not supported");
    }

    /**
     * Fetch the next entry if there is one.
     */
    @SuppressWarnings("unchecked")
    private void fetchNext() {
        while(cursorPos != null) {
            Page page = cursorPos.page;
            if(!snapshot && page.isRemoved()) {
                cursorPos = traverseDown(map.getRootPage(), anchor);
                if(last != null) {
                    ++cursorPos.index;
                }
            } else if (page.isLeaf()) {
                if (cursorPos.index < page.getKeyCount()) {
                    int index = cursorPos.index++;
                    current = (K) page.getKey(index);
                    currentValue = (V) page.getValue(index);
                    lastPage = page;
                    assert last != current : last + " == " + current;
                    return;
                }
                cursorPos = cursorPos.parent;
            } else {
                int index = ++cursorPos.index;
                if (index < map.getChildPageCount(page)) {
                    cursorPos = traverseDown(page.getChildPage(index), null, cursorPos);
                } else {
                    cursorPos = cursorPos.parent;
                }
            }
        }
        current = null;
    }

    private static CursorPos traverseDown(Page p, Object key) {
        return traverseDown(p, key, null);
    }

    private static CursorPos traverseDown(Page p, Object key, CursorPos pos) {
        while (!p.isLeaf()) {
            assert p.getKeyCount() > 0;
            int index = 0;
            if(key != null) {
                index = p.binarySearch(key) + 1;
                if (index < 0) {
                    index = -index;
                }
            }
            pos = new CursorPos(p, index, pos);
            p = p.getChildPage(index);
        }
        int index = 0;
        if(key != null) {
            index = p.binarySearch(key);
            if (index < 0) {
                index = -index - 1;
            }
        }
        return new CursorPos(p, index, pos);
    }
}
