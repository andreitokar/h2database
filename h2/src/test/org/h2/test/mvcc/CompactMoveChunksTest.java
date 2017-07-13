package org.h2.test.mvcc;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.h2.mvstore.MVStore;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class CompactMoveChunksTest {

    @ClassRule
    public static TemporaryFolder folder = new TemporaryFolder();

    private static String storePath;

    private MVStore store;
    private Map<Long, Map<String, Object>> map;

    @BeforeClass
    public static void beforeClass() throws IOException {
        storePath = folder.newFolder().getPath();
    }

    public void openStore() throws Exception {
        File file = new File(storePath, "test.db");

        MVStore.Builder storeBuilder = new MVStore.Builder();
        storeBuilder.autoCommitDisabled();
        storeBuilder.fileName(file.getPath());

        store = storeBuilder.open();
        store.setReuseSpace(true);
        store.setVersionsToKeep(0);
    }

    public void closeStore() throws Exception {
        store.rollback();
        store.compactMoveChunks();
        store.close();
    }

    private void openMap() throws Exception {
        map = store.openMap("map");
    }

    @Test
    public void testPut() throws Exception {
        Map<String, Object> values = new HashMap<>();
        values.put("mapKey", "mapValue");

        openStore();
        openMap();
        map.put(11L, values);
        store.commit();
        closeStore();

        openStore();
        openMap();
        Map<String, Object> content = map.get(11L);
        assertNotNull(content);
        assertTrue(content.toString(), content.containsKey("mapKey"));
        assertTrue(content.toString(), content.containsValue("mapValue"));

        closeStore();
    }

}