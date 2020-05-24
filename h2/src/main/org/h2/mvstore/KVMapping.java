/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
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
    public final boolean existing;

    public KVMapping(K key, V value) {
        this(key, value, value == null);
    }
    public KVMapping(K key, V value, boolean existing) {
        assert key != null;
        assert value != null || existing;
        this.key = key;
        this.value = value;
        this.existing = existing;
    }

    @Override
    public String toString() {
        return "KVMapping{" +
                "key=" + key +
                ", value=" + value +
                (existing ? ", existing" : ", new") +
                '}';
    }
}
