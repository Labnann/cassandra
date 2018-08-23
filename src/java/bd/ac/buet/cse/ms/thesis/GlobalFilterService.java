package bd.ac.buet.cse.ms.thesis;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.commons.lang.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.IFilter;
import org.apache.hadoop.util.hash.MurmurHash;

public class GlobalFilterService {

    private static final Logger logger = LoggerFactory.getLogger(GlobalFilterService.class);

    private static final String[] SYSTEM_KEYSPACES = new String[]{ "system", "system_distributed", "system_schema",
                                                                  "system_auth", "system_traces" };

    private static final String GLOBAL_FILTER_FILE_NAME = "Filters.db";
    private static final int FILTER_NUM_OF_ELEMENTS = 1000;
    private static final double FILTER_FALSE_POSITIVE_RATE = 0.01;

    private static GlobalFilterService service;

    private HashMap<String /* IP */, HashMap<String /* KeySpace */, HashMap<String /* ColumnFamily */, IFilter>>> tableFilters = new HashMap<>();

    private long lastSavedFilterHash = 0;
    private String nodeIp = null;

    public static boolean isSystemKeyspace(String ksName) {
        return Arrays.asList(SYSTEM_KEYSPACES).contains(ksName);
    }

    public static synchronized void initialize() {
        if (service != null) {
            return;
        }

        service = new GlobalFilterService();
        service.nodeIp = FBUtilities.getBroadcastAddress().toString();
        service.loadFiltersFromDisk();
    }

    public static synchronized GlobalFilterService instance() {
        if (service == null) {
            throw new RuntimeException("Global Filters not initialized.");
        }

        return service;
    }

    public void add(DecoratedKey key, String columnFamily, String keySpace) {
        if (!FilterSwitch.ENABLE_GLOBAL_FILTER) {
            return;
        }

        if (isSystemKeyspace(keySpace)) {
            logger.trace("Ignoring adding key to global filter for system keyspace {}", keySpace);

            return;
        }

        if (!tableFilters.containsKey(nodeIp)) {
            tableFilters.put(nodeIp, new HashMap<>());
        }

        HashMap<String, HashMap<String, IFilter>> ksMap = tableFilters.get(nodeIp);
        if (!ksMap.containsKey(keySpace)) {
            ksMap.put(keySpace, new HashMap<>());
        }

        HashMap<String, IFilter> cfFilterMap = ksMap.get(keySpace);
        if (!cfFilterMap.containsKey(columnFamily)) {
            cfFilterMap.put(columnFamily,
                            FilterFactory.getFilter(FILTER_NUM_OF_ELEMENTS, FILTER_FALSE_POSITIVE_RATE, false, false, true));
        }

        cfFilterMap.get(columnFamily).add(key);
    }

    public void delete(DecoratedKey key, String columnFamily, String keySpace) {
        if (!FilterSwitch.ENABLE_GLOBAL_FILTER) {
            return;
        }

        if (isSystemKeyspace(keySpace)) {
            logger.trace("Ignoring deleting key from global filter for system keyspace {}", keySpace);

            return;
        }

        if (!tableFilters.containsKey(nodeIp)) {
            logger.warn("Tried to delete key from nonexistent filter. IP: {}", nodeIp);

            return;
        }

        HashMap<String, HashMap<String, IFilter>> ksMap = tableFilters.get(nodeIp);
        if (!ksMap.containsKey(keySpace)) {
            logger.warn("Tried to delete key from nonexistent filter. KeySpace: {}", keySpace);

            return;
        }

        HashMap<String, IFilter> cfFilterMap = ksMap.get(keySpace);
        if (!cfFilterMap.containsKey(columnFamily)) {
            logger.warn("Tried to delete key from nonexistent filter. ColumnFamily: {}", columnFamily);

            return;
        }

        cfFilterMap.get(columnFamily).delete(key);
    }

    public boolean isPresent(DecoratedKey key, String columnFamily, String keySpace, String ip) {
        if (isSystemKeyspace(keySpace)) {
            throw new RuntimeException("Key lookup from system keyspace (" + keySpace + ") is not allowed.");
        }

        if (!tableFilters.containsKey(ip)) {
            logger.warn("Tried to lookup key from nonexistent filter. IP: {}", ip);

            return false;
        }

        HashMap<String, HashMap<String, IFilter>> ksMap = tableFilters.get(ip);
        if (ksMap == null || !ksMap.containsKey(keySpace)) {
            logger.warn("Tried to lookup key from nonexistent filter. KeySpace: {}", keySpace);

            return false;
        }

        HashMap<String, IFilter> cfFilterMap = ksMap.get(keySpace);
        if (cfFilterMap == null || !cfFilterMap.containsKey(columnFamily)) {
            logger.warn("Tried to lookup key from nonexistent filter. ColumnFamily: {}", columnFamily);

            return false;
        }

        return cfFilterMap.get(columnFamily).isPresent(key);
    }

    public HashMap<String, HashMap<String, IFilter>> getFilters(String ip) {
        return tableFilters.get(ip);
    }

    public void sync(String ip, HashMap<String, HashMap<String, IFilter>> filters) {
        logger.info("SyncedFilterHashesDifferent={}", tableFilters.get(ip) == null || tableFilters.get(ip).hashCode() != filters.hashCode());
        tableFilters.put(ip, filters);
        saveFiltersToDisk(false);
    }

    private GlobalFilterService() {
        // ignored
    }

    private void loadFiltersFromDisk() {
        if (!FilterSwitch.ENABLE_GLOBAL_FILTER) {
            return;
        }

        Path globalFiltersPath = getGlobalFiltersPath();

        logger.debug("Global Filters Path: {}", globalFiltersPath.toAbsolutePath());

        if (globalFiltersPath.toFile().exists()) {
            try {
                byte[] filterBytes = Files.readAllBytes(globalFiltersPath);
                //noinspection unchecked
                tableFilters = (HashMap<String, HashMap<String,HashMap<String,IFilter>>>) SerializationUtils.deserialize(filterBytes);

                if (FilterSwitch.globalFilter == FilterSwitch.BLOOM_FILTER) {
                    for (HashMap<String, HashMap<String, IFilter>> values : tableFilters.values()) {
                        for (HashMap<String, IFilter> vals : values.values()) {
                            for (IFilter filter : vals.values()) {
                                ((BloomFilter) filter).restoreInstantiation();
                            }
                        }
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("Cannot load Global Filters", e);
            }

            logger.debug("Loaded Global Filters from disk");
        }
        else {
            logger.debug("Global Filters does not exist on disk.");
        }
    }

    public void saveFiltersToDisk() {
        saveFiltersToDisk(false);
    }

    private void saveFiltersToDisk(boolean forceSync) {
        if (!FilterSwitch.ENABLE_GLOBAL_FILTER) {
            return;
        }

        new Thread() {
            public void run() {
                byte[] bytes = SerializationUtils.serialize(tableFilters);

                int hash = MurmurHash.getInstance().hash(bytes);

                if (hash == lastSavedFilterHash) {
                    logger.debug("Skipping saving Global Filters as apparently it hasn't changed.");

                    return;
                }

                lastSavedFilterHash = hash;

                try {
                    Files.write(getGlobalFiltersPath(), bytes);
                } catch (Exception e) {
                    throw new RuntimeException("Cannot save Global Filter", e);
                }

                logger.debug("Saved Global Filters to disk. Filter serialized size: {}", bytes.length);

                if (forceSync) {
                    GlobalFilterSyncService.forceSyncNow();
                }
            }
        }.start();
    }

    private Path getGlobalFiltersPath() {
        String dataDir = DatabaseDescriptor.getAllDataFileLocations()[0];
        String storageDir = dataDir.substring(0, dataDir.length() - 5);

        return Paths.get(storageDir + '/' + GLOBAL_FILTER_FILE_NAME);
    }
}
