/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import org.h2.mvstore.type.DataType;

/**
 * A position in a cursor.
 * Instance represents a node in the linked list, which traces path
 * from a specific (target) key within a leaf node all the way up to te root
 * (bottom up path).
 */
public final class CursorPos<K,V> {

    /**
     * The page at the current level.
     */
    public Page<K,V> page;

    /**
     * Index of the key (within page above) used to go down to a lower level
     * in case of intermediate nodes, or index of the target key for leaf a node.
     * In a later case, it could be negative, if the key is not present.
     */
    public int index;

    /**
     * Next node in the linked list, representing the position within parent level,
     * or null, if we are at the root level already.
     */
    public CursorPos<K,V> parent;


    private CursorPos() {}

    public CursorPos(Page<K,V> page, int index, CursorPos<K,V> parent) {
        this.page = page;
        this.index = index;
        this.parent = parent;
    }

    /**
     * Searches for a given key and creates a breadcrumb trail through a B-tree
     * rooted at a given Page. Resulting path starts at "insertion point" for a
     * given key and goes back to the root.
     *
     * @param <K> key type
     * @param <V> value type
     *
     * @param page      root of the tree
     * @param key       the key to search for
     * @return head of the CursorPos chain (insertion point)
     */
    static <K,V> CursorPos<K,V> traverseDown(Page<K,V> page, K key) {
        CursorPos<K,V> cursorPos = null;
        while (!page.isLeaf()) {
            int index = page.binarySearch(key);
            index = index < 0 ? ~index : index + 1;
            cursorPos = new CursorPos<>(page, index, cursorPos);
            page = page.getChildPage(index);
        }
        return new CursorPos<>(page, page.binarySearch(key), cursorPos);
    }

    /**
     * Searches for a given key and creates a breadcrumb trail through a B-tree
     * rooted at a given Page. Resulting path starts at "insertion point" for a
     * given key and goes back to the root.
     *
     * @param page      root of the tree
     * @param key       the key to search for
     * @param lastPath  path of a previous traversal for the same key, that can be re-used
     *                  to minimize object creation and/or number of key comparisons
     * @return head of the CursorPos chain (insertion point)
     */
    static <K,V> CursorPos<K,V> traverseDown(Page<K,V> page, K key, CursorPos<K,V> lastPath) {
        lastPath = reverse(lastPath, null);
        DataType<K> comparator = page.map.getKeyType();
        CursorPos<K,V> cursorPos = null;
        boolean diverge = false;
        while (true) {
            if (lastPath == null) {
                lastPath = new CursorPos<>();
                diverge = true;
            }
            int index = lastPath.index;
            boolean isLeaf = page.isLeaf();
            if (!diverge) {
                if (page == lastPath.page) {
                    return reverse(lastPath, cursorPos);
                }
                assert lastPath.page.isLeaf() || index >= 0;
                diverge = isLeaf != lastPath.page.isLeaf();
                if (!diverge) {
                    int keyCount = page.getKeyCount();
                    if (!isLeaf) {
                        diverge = index > keyCount ||
                                index > 0 && comparator.compare(page.getKey(index - 1), key) > 0 ||
                                index < keyCount && comparator.compare(key, page.getKey(index)) >= 0;

                    } else if(index >= 0) {
                        diverge = index >= keyCount || comparator.compare(page.getKey(index), key) != 0;
                    } else {
                        int indx = ~index;
                        diverge = indx > keyCount ||
                            indx > 0 && comparator.compare(page.getKey(indx - 1), key) >= 0 ||
                            indx < keyCount && comparator.compare(key, page.getKey(indx)) >= 0;
                    }
                }
            }
            CursorPos<K, V> tmp = lastPath.parent;
            lastPath.parent = cursorPos;
            cursorPos = lastPath;
            lastPath = tmp;

            cursorPos.page = page;
            if (diverge) {
                index = page.binarySearch(key);
                if (!isLeaf) {
                    index = index < 0 ? ~index : index + 1;
                }
                cursorPos.index = index;
            }
            if (isLeaf) {
                return cursorPos;
            }
            page = page.getChildPage(index);
        }
    }

    private static <K,V> CursorPos<K,V> reverse(CursorPos<K,V> cursorPos, CursorPos<K,V> res) {
        while (cursorPos != null) {
            CursorPos<K, V> tmp = cursorPos.parent;
            cursorPos.parent = res;
            res = cursorPos;
            cursorPos = tmp;
        }
        return res;
    }

    /**
     * Calculate the memory used by changes that are not yet stored.
     *
     * @param version the version
     * @return the amount of memory
     */
    int processRemovalInfo(long version) {
        int unsavedMemory = 0;
        for (CursorPos<K,V> head = this; head != null; head = head.parent) {
            unsavedMemory += head.page.removePage(version);
        }
        return unsavedMemory;
    }

    @Override
    public String toString() {
        return "CursorPos{" +
                "page=" + page +
                ", index=" + index +
                ", parent=" + parent +
                '}';
    }
}

