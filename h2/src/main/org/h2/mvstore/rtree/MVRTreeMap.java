/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.rtree;

import java.util.ArrayList;
import java.util.Iterator;

import org.h2.mvstore.CursorPos;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.Page;
import org.h2.mvstore.type.DataType;
import org.h2.mvstore.type.ObjectDataType;
import org.h2.util.New;

/**
 * An r-tree implementation. It supports both the linear and the quadratic split
 * algorithm.
 *
 * @param <V> the value class
 */
public final class MVRTreeMap<V> extends MVMap<SpatialKey, V> {

    /**
     * The spatial key type.
     */
    private final SpatialDataType keyType;

    private boolean quadraticSplit;

    public MVRTreeMap(int dimensions, DataType valueType) {
        this(new SpatialDataType(dimensions), valueType);
    }

    private MVRTreeMap(SpatialDataType keyType, DataType valueType) {
        super(keyType, valueType);
        this.keyType = keyType;
    }

    public MVRTreeMap<V>cloneFromThis() {
        MVRTreeMap<V> res = new MVRTreeMap<>(keyType, getValueType());
        res.quadraticSplit = quadraticSplit;
        return res;
    }

    /**
     * Create a new map with the given dimensions and value type.
     *
     * @param <V> the value type
     * @param dimensions the number of dimensions
     * @param valueType the value type
     * @return the map
     */
    public static <V> MVRTreeMap<V> create(int dimensions, DataType valueType) {
        return new MVRTreeMap<V>(dimensions, valueType);
    }

    @Override
    @SuppressWarnings("unchecked")
    public V get(Object key) {
        V result = (V) get(getRootPage(), key);
        return result;
    }

    /**
     * Iterate over all keys that have an intersection with the given rectangle.
     *
     * @param x the rectangle
     * @return the iterator
     */
    public RTreeCursor findIntersectingKeys(SpatialKey x) {
        return new RTreeCursor(getRootPage(), x) {
            @Override
            protected boolean check(boolean leaf, SpatialKey key,
                    SpatialKey test) {
                return keyType.isOverlap(key, test);
            }
        };
    }

    /**
     * Iterate over all keys that are fully contained within the given
     * rectangle.
     *
     * @param x the rectangle
     * @return the iterator
     */
    public RTreeCursor findContainedKeys(SpatialKey x) {
        return new RTreeCursor(getRootPage(), x) {
            @Override
            protected boolean check(boolean leaf, SpatialKey key,
                    SpatialKey test) {
                if (leaf) {
                    return keyType.isInside(key, test);
                }
                return keyType.isOverlap(key, test);
            }
        };
    }

    private boolean contains(Page p, int index, Object key) {
        return keyType.contains(p.getKey(index), key);
    }

    /**
     * Get the object for the given key. An exact match is required.
     *
     * @param p the page
     * @param key the key
     * @return the value, or null if not found
     */
    protected Object get(Page p, Object key) {
        if (!p.isLeaf()) {
            for (int i = 0; i < p.getKeyCount(); i++) {
                if (contains(p, i, key)) {
                    Object o = get(p.getChildPage(i), key);
                    if (o != null) {
                        return o;
                    }
                }
            }
        } else {
            for (int i = 0; i < p.getKeyCount(); i++) {
                if (keyType.equals(p.getKey(i), key)) {
                    return p.getValue(i);
                }
            }
        }
        return null;
    }

    /**
     * Remove a key-value pair, if the key exists.
     *
     * @param key the key (may not be null)
     * @return the old value if the key existed, or null otherwise
     */
    @Override
    @SuppressWarnings("unchecked")
    public V remove(Object key) {
        return operate((SpatialKey) key, null, null);
    }

    @Override
    public V operate(SpatialKey key, V value, DecisionMaker<? super V> decisionMaker) {
        beforeWrite();
        V result;
        RootReference rootReference;
        Page p;
        long version;
        do {
            version = writeVersion;
            rootReference = getRoot();
            p = rootReference.root.copy(version, true);
            result = operate(p, version, key, value, decisionMaker);
            Decision decision = decisionMaker != null ? decisionMaker.decide(result, value) :
                                value == null         ? Decision.REMOVE :
                                                        Decision.PUT;
            switch (decision) {
                case ABORT:
                    return result;
                case REMOVE:
                    if (!p.isLeaf() && p.getTotalCount() == 0) {
                        p.removePage();
                        p = Page.createEmpty(this, version);
                    }
                    break;
                case PUT:
                    if (p.getMemory() > store.getPageSplitSize() && p.getKeyCount() > 3) {
                        // only possible if this is the root, else we would have
                        // split earlier (this requires pageSplitSize is fixed)
                        long totalCount = p.getTotalCount();
                        Page split = split(p, version);
                        Object k1 = getBounds(p);
                        Object k2 = getBounds(split);
                        Object[] keys = {k1, k2};
                        Page.PageReference[] children = {
                                new Page.PageReference(p),
                                new Page.PageReference(split),
                                Page.PageReference.EMPTY
                        };
                        p = Page.create(this, version,
                                keys, null,
                                children,
                                totalCount, 0);
                        if(store.getFileStore() != null) {
                            store.registerUnsavedPage(p.getMemory());
                        }
                    }
                    break;
            }
        } while(!newRoot(rootReference, p, version));
        return result;
    }

    private V operate(Page p, long version, Object key, Object value, DecisionMaker decisionMaker) {
        V result = null;
        if (p.isLeaf()) {
            int indx = -1;
            for (int i = 0; i < p.getKeyCount(); i++) {
                if (keyType.equals(p.getKey(i), key)) {
                    indx = i;
                }
            }
            result = indx < 0 ? null : (V)p.getValue(indx);
            Decision decision = decisionMaker != null ? decisionMaker.decide(result, value) :
                                value == null         ? Decision.REMOVE :
                                                        Decision.PUT;
            if(decisionMaker != null) {
                value = decisionMaker.selectValue(result, value);
            }
            switch (decision) {
                case ABORT: break;
                case REMOVE:
                    if(indx >= 0) {
                        p.remove(indx);
                    }
                    break;
                case PUT:
                    if(indx < 0) {
                        p.insertLeaf(p.getKeyCount(), key, value);
                    } else {
                        p.setKey(indx, key);
                        p.setValue(indx, value);
                    }
                    break;
            }
            return result;
        }

        // p is a node
        if(value == null)
        {
            for (int i = 0; i < p.getKeyCount(); i++) {
                if (contains(p, i, key)) {
                    Page cOld = p.getChildPage(i);
                    // this will mark the old page as deleted
                    // so we need to update the parent in any case
                    // (otherwise the old page might be deleted again)
                    Page c = cOld.copy(version, true);
                    long oldSize = c.getTotalCount();
                    result = operate(c, version, key, value, decisionMaker);
                    p.setChild(i, c);
                    if (oldSize == c.getTotalCount()) {
                        continue;
                    }
                    if (c.getTotalCount() == 0) {
                        // this child was deleted
                        p.remove(i);
                        if (p.getKeyCount() == 0) {
                            c.removePage();
                        }
                        break;
                    }
                    Object oldBounds = p.getKey(i);
                    if (!keyType.isInside(key, oldBounds)) {
                        p.setKey(i, getBounds(c));
                    }
                    break;
                }
            }
        } else {
            int index = -1;
            for (int i = 0; i < p.getKeyCount(); i++) {
                if (contains(p, i, key)) {
                    Page c = p.getChildPage(i);
                    if(get(c, key) != null) {
                        index = i;
                        break;
                    }
                    if(index < 0) {
                        index = i;
                    }
                }
            }
            if (index < 0) {
                // a new entry, we don't know where to add yet
                float min = Float.MAX_VALUE;
                for (int i = 0; i < p.getKeyCount(); i++) {
                    Object k = p.getKey(i);
                    float areaIncrease = keyType.getAreaIncrease(k, key);
                    if (areaIncrease < min) {
                        index = i;
                        min = areaIncrease;
                    }
                }
            }
            Page c = p.getChildPage(index).copy(version, true);
            if (c.getMemory() > store.getPageSplitSize() && c.getKeyCount() > 4) {
                // split on the way down
                Page split = split(c, version);
                p.setKey(index, getBounds(c));
                p.setChild(index, c);
                p.insertNode(index, getBounds(split), split);
                // now we are not sure where to add
                result = operate(p, version, key, value, decisionMaker);
            } else {
                result = operate(c, version, key, value, decisionMaker);
                Object bounds = p.getKey(index);
                keyType.increaseBounds(bounds, key);
                p.setKey(index, bounds);
                p.setChild(index, c);
            }
        }
        return result;
    }

    private Object getBounds(Page x) {
        Object bounds = keyType.createBoundingBox(x.getKey(0));
        for (int i = 1; i < x.getKeyCount(); i++) {
            keyType.increaseBounds(bounds, x.getKey(i));
        }
        return bounds;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V put(SpatialKey key, V value) {
        return operate(key, value, null);
    }

    /**
     * Add a given key-value pair. The key should not exist (if it exists, the
     * result is undefined).
     *
     * @param key the key
     * @param value the value
     */
    public void add(SpatialKey key, V value) {
        operate(key, value, null);
    }

    private Page split(Page p, long writeVersion) {
        return quadraticSplit ?
                splitQuadratic(p, writeVersion) :
                splitLinear(p, writeVersion);
    }

    private Page splitLinear(Page p, long writeVersion) {
        ArrayList<Object> keys = New.arrayList();
        for (int i = 0; i < p.getKeyCount(); i++) {
            keys.add(p.getKey(i));
        }
        int[] extremes = keyType.getExtremes(keys);
        if (extremes == null) {
            return splitQuadratic(p, writeVersion);
        }
        Page splitA = newPage(p.isLeaf(), writeVersion);
        Page splitB = newPage(p.isLeaf(), writeVersion);
        move(p, splitA, extremes[0]);
        if (extremes[1] > extremes[0]) {
            extremes[1]--;
        }
        move(p, splitB, extremes[1]);
        Object boundsA = keyType.createBoundingBox(splitA.getKey(0));
        Object boundsB = keyType.createBoundingBox(splitB.getKey(0));
        while (p.getKeyCount() > 0) {
            Object o = p.getKey(0);
            float a = keyType.getAreaIncrease(boundsA, o);
            float b = keyType.getAreaIncrease(boundsB, o);
            if (a < b) {
                keyType.increaseBounds(boundsA, o);
                move(p, splitA, 0);
            } else {
                keyType.increaseBounds(boundsB, o);
                move(p, splitB, 0);
            }
        }
        while (splitB.getKeyCount() > 0) {
            move(splitB, p, 0);
        }
        return splitA;
    }

    private Page splitQuadratic(Page p, long writeVersion) {
        Page splitA = newPage(p.isLeaf(), writeVersion);
        Page splitB = newPage(p.isLeaf(), writeVersion);
        float largest = Float.MIN_VALUE;
        int ia = 0, ib = 0;
        for (int a = 0; a < p.getKeyCount(); a++) {
            Object objA = p.getKey(a);
            for (int b = 0; b < p.getKeyCount(); b++) {
                if (a == b) {
                    continue;
                }
                Object objB = p.getKey(b);
                float area = keyType.getCombinedArea(objA, objB);
                if (area > largest) {
                    largest = area;
                    ia = a;
                    ib = b;
                }
            }
        }
        move(p, splitA, ia);
        if (ia < ib) {
            ib--;
        }
        move(p, splitB, ib);
        Object boundsA = keyType.createBoundingBox(splitA.getKey(0));
        Object boundsB = keyType.createBoundingBox(splitB.getKey(0));
        while (p.getKeyCount() > 0) {
            float diff = 0, bestA = 0, bestB = 0;
            int best = 0;
            for (int i = 0; i < p.getKeyCount(); i++) {
                Object o = p.getKey(i);
                float incA = keyType.getAreaIncrease(boundsA, o);
                float incB = keyType.getAreaIncrease(boundsB, o);
                float d = Math.abs(incA - incB);
                if (d > diff) {
                    diff = d;
                    bestA = incA;
                    bestB = incB;
                    best = i;
                }
            }
            if (bestA < bestB) {
                keyType.increaseBounds(boundsA, p.getKey(best));
                move(p, splitA, best);
            } else {
                keyType.increaseBounds(boundsB, p.getKey(best));
                move(p, splitB, best);
            }
        }
        while (splitB.getKeyCount() > 0) {
            move(splitB, p, 0);
        }
        return splitA;
    }

    private Page newPage(boolean leaf, long writeVersion) {
        Object[] values;
        Page.PageReference[] refs;
        if (leaf) {
            values = Page.EMPTY_OBJECT_ARRAY;
            refs = null;
        } else {
            values = null;
            refs = Page.PageReference.SINGLE_EMPTY;
        }
        Page page = Page.create(this, writeVersion,
                Page.EMPTY_OBJECT_ARRAY, values,
                refs, 0, 0);
        if(store.getFileStore() != null)
        {
            store.registerUnsavedPage(page.getMemory());
        }
        return page;
    }

    private static void move(Page source, Page target, int sourceIndex) {
        Object k = source.getKey(sourceIndex);
        if (source.isLeaf()) {
            Object v = source.getValue(sourceIndex);
            target.insertLeaf(0, k, v);
        } else {
            Page c = source.getChildPage(sourceIndex);
            target.insertNode(0, k, c);
        }
        source.remove(sourceIndex);
    }

    /**
     * Add all node keys (including internal bounds) to the given list.
     * This is mainly used to visualize the internal splits.
     *
     * @param list the list
     * @param p the root page
     */
    public void addNodeKeys(ArrayList<SpatialKey> list, Page p) {
        if (p != null && !p.isLeaf()) {
            for (int i = 0; i < p.getKeyCount(); i++) {
                list.add((SpatialKey) p.getKey(i));
                addNodeKeys(list, p.getChildPage(i));
            }
        }
    }

    public boolean isQuadraticSplit() {
        return quadraticSplit;
    }

    public void setQuadraticSplit(boolean quadraticSplit) {
        this.quadraticSplit = quadraticSplit;
    }

    @Override
    protected int getChildPageCount(Page p) {
        return p.getRawChildPageCount() - 1;
    }

    /**
     * A cursor to iterate over a subset of the keys.
     */
    public static class RTreeCursor implements Iterator<SpatialKey> {

        private final SpatialKey filter;
        private CursorPos pos;
        private SpatialKey current;
        private final Page root;
        private boolean initialized;

        protected RTreeCursor(Page root, SpatialKey filter) {
            this.root = root;
            this.filter = filter;
        }

        @Override
        public boolean hasNext() {
            if (!initialized) {
                // init
                pos = new CursorPos(root, 0, null);
                fetchNext();
                initialized = true;
            }
            return current != null;
        }

        /**
         * Skip over that many entries. This method is relatively fast (for this
         * map implementation) even if many entries need to be skipped.
         *
         * @param n the number of entries to skip
         */
        public void skip(long n) {
            while (hasNext() && n-- > 0) {
                fetchNext();
            }
        }

        @Override
        public SpatialKey next() {
            if (!hasNext()) {
                return null;
            }
            SpatialKey c = current;
            fetchNext();
            return c;
        }

        @Override
        public void remove() {
            throw DataUtils.newUnsupportedOperationException(
                    "Removing is not supported");
        }

        /**
         * Fetch the next entry if there is one.
         */
        protected void fetchNext() {
            while (pos != null) {
                Page p = pos.page;
                if (p.isLeaf()) {
                    while (pos.index < p.getKeyCount()) {
                        SpatialKey c = (SpatialKey) p.getKey(pos.index++);
                        if (filter == null || check(true, c, filter)) {
                            current = c;
                            return;
                        }
                    }
                } else {
                    boolean found = false;
                    while (pos.index < p.getKeyCount()) {
                        int index = pos.index++;
                        SpatialKey c = (SpatialKey) p.getKey(index);
                        if (filter == null || check(false, c, filter)) {
                            Page child = pos.page.getChildPage(index);
                            pos = new CursorPos(child, 0, pos);
                            found = true;
                            break;
                        }
                    }
                    if (found) {
                        continue;
                    }
                }
                // parent
                pos = pos.parent;
            }
            current = null;
        }

        /**
         * Check a given key.
         *
         * @param leaf if the key is from a leaf page
         * @param key the stored key
         * @param test the user-supplied test key
         * @return true if there is a match
         */
        protected boolean check(boolean leaf, SpatialKey key, SpatialKey test) {
            return true;
        }

    }

    @Override
    public String getType() {
        return "rtree";
    }

    /**
     * A builder for this class.
     *
     * @param <V> the value type
     */
    public static class Builder<V> implements
            MVMap.MapBuilder<MVRTreeMap<V>, SpatialKey, V> {

        private int dimensions = 2;
        private DataType valueType;

        /**
         * Create a new builder for maps with 2 dimensions.
         */
        public Builder() {
            // default
        }

        /**
         * Set the dimensions.
         *
         * @param dimensions the dimensions to use
         * @return this
         */
        public Builder<V> dimensions(int dimensions) {
            this.dimensions = dimensions;
            return this;
        }

        /**
         * Set the key data type.
         *
         * @param valueType the key type
         * @return this
         */
        public Builder<V> valueType(DataType valueType) {
            this.valueType = valueType;
            return this;
        }

        @Override
        public MVRTreeMap<V> create() {
            if (valueType == null) {
                valueType = new ObjectDataType();
            }
            return new MVRTreeMap<V>(dimensions, valueType);
        }

    }

}
