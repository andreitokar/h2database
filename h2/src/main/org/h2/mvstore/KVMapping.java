/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

/**
 * Class KVMapping.
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public final class KVMapping<K,V>
{
    public final K   key;
    public final V   value;
    public final int index;

    KVMapping(K key, V value, int index) {
        assert key != null;
        assert value != null || index >= 0;
        this.key = key;
        this.value = value;
        this.index = index;
    }

    @Override
    public String toString() {
        return "KVMapping{" +
                "key=" + key +
                ", index=" + index +
                ", value=" + value +
                '}';
    }
}
