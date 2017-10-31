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
    private final Page        root;
    private       CursorPos   pos;
    private       K           current;
    private       K           last;
    private       V           lastValue;
    private       Page        lastPage;

    Cursor(MVMap<K, ?> map, Page root, K from) {
        this.map = map;
        this.root = root;
        min(root, from);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean hasNext() {
        if (current != null) {
            return true;
        }
        while (pos != null) {
            Page page = pos.page;
            int index = pos.index;
            if (index >= (page.isLeaf() ? page.getKeyCount() : map.getChildPageCount(page))) {
                pos = pos.parent;
                if(pos == null)
                {
                    return false;
                }
                ++pos.index;
            } else {
                while (!page.isLeaf()) {
                    page = page.getChildPage(index);
                    pos = new CursorPos(page, 0, pos);
                    index = 0;
                }
                current = last = (K) page.getKey(index);
                lastValue = (V) page.getValue(index);
                lastPage = page;
                ++pos.index;
                return true;
            }
        }
        return false;
    }

    @Override
    public K next() {
        if (!hasNext()) {
            return null;
        }
        K tmp = current;
        current = null;
        return tmp;
    }

    /**
     * Get the last read key if there was one.
     *
     * @return the key or null
     */
    public K getKey() {
        return last;
    }

    /**
     * Get the last read value if there was one.
     *
     * @return the value or null
     */
    public V getValue() {
        return lastValue;
    }

    /**
     * Get the page where last retrieved key is located.
     *
     * @return the page
     */
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
        if (!hasNext()) {
            return;
        }
        if (n < 10) {
            while (n-- > 0 && hasNext()) {
                next();
            }
        } else {
            long index = map.getKeyIndex(current);
            K k = map.getKey(index + n);
            pos = null;
            min(root, k);
            current = null;
        }
    }

    @Override
    public void remove() {
        throw DataUtils.newUnsupportedOperationException(
                "Removing is not supported");
    }

    /**
     * Fetch the next entry that is equal or larger than the given key, starting
     * from the given page. This method retains the stack.
     *
     * @param p the page to start
     * @param key the key to search
     */
    private void min(Page p, K key) {
        while (!p.isLeaf()) {
            int index = 0;
            if (key != null) {
                index = p.binarySearch(key) + 1;
                if (index < 0) {
                    index = -index;
                }
            }
            pos = new CursorPos(p, index, pos);
            p = p.getChildPage(index);
        }
        int index = 0;
        if (key != null) {
            index =  p.binarySearch(key);
            if (index < 0) {
                index = -index - 1;
            }
        }
        pos = new CursorPos(p, index, pos);
    }
}
