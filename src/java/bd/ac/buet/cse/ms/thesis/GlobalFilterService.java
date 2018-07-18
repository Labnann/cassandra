package bd.ac.buet.cse.ms.thesis;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.utils.IFilter;

public class GlobalFilterService {

    private static final Logger logger = LoggerFactory.getLogger(GlobalFilterService.class);

    private static final String[] SYSTEM_KEYSPACES = new String[]{ "system", "system_distributed", "system_schema",
                                                                  "system_auth", "system_traces" };

    private static final String GLOBAL_FILTER_FILE_NAME = "Filters.db";
    private static final long FILTER_DIRTY_MONITORING_INTERVAL_MILLIS = 5 * 1000;

    private static GlobalFilterService service;

    private HashMap<String /* KeySpace */, HashMap<String /* ColumnFamily */, IFilter>> tableFilters = new HashMap<>();

    private boolean dirty = false;
    private boolean stopService = false;

    private final FilterMonitor filterMonitor = new FilterMonitor();

    private final Lock lock = new ReentrantLock();

    public static boolean isSystemKeyspace(String ksName) {
        return Arrays.asList(SYSTEM_KEYSPACES).contains(ksName);
    }

    public static synchronized void initialize() {
        if (service != null) {
            return;
        }

        service = new GlobalFilterService();
        service.loadFiltersFromDisk();
        service.startFilterDirtyMonitor();
    }

    public static synchronized void destroy() {
        service.stopService = true;
        service.filterMonitor.interrupt();
    }

    public static synchronized GlobalFilterService instance() {
        if (service == null) {
            throw new RuntimeException("Global Filters not initialized.");
        }

        return service;
    }

    public void add(DecoratedKey key, String columnFamily, String keySpace) {
        if (isSystemKeyspace(keySpace)) {
            logger.trace("Ignoring adding key to global filter for system keyspace {}", keySpace);

            return;
        }

        if (!tableFilters.containsKey(keySpace)) {
            tableFilters.put(keySpace, new HashMap<>());
        }

        HashMap<String, IFilter> cfFilterMap = tableFilters.get(keySpace);
        if (!cfFilterMap.containsKey(columnFamily)) {
            cfFilterMap.put(columnFamily, createFilter());
        }

        cfFilterMap.get(columnFamily).add(key);

        markDirty();
    }

    public void delete(DecoratedKey key, String columnFamily, String keySpace) {
        if (isSystemKeyspace(keySpace)) {
            logger.trace("Ignoring deleting key from global filter for system keyspace {}", keySpace);

            return;
        }

        if (!tableFilters.containsKey(keySpace)) {
            logger.warn("Tried to delete key from nonexistent filter. KeySpace: {}", keySpace);

            return;
        }

        HashMap<String, IFilter> cfFilterMap = tableFilters.get(keySpace);
        if (!cfFilterMap.containsKey(columnFamily)) {
            logger.warn("Tried to delete key from nonexistent filter. ColumnFamily: {}", columnFamily);

            return;
        }

        cfFilterMap.get(columnFamily).delete(key);

        markDirty();
    }

    public boolean isPresent(DecoratedKey key, String columnFamily, String keySpace) {
        if (isSystemKeyspace(keySpace)) {
            throw new RuntimeException("Key lookup from system keyspace (" + keySpace + ") is not allowed.");
        }

        if (!tableFilters.containsKey(keySpace)) {
            logger.warn("Tried to lookup key from nonexistent filter. KeySpace: {}", keySpace);

            return false;
        }

        HashMap<String, IFilter> cfFilterMap = tableFilters.get(keySpace);
        if (!cfFilterMap.containsKey(columnFamily)) {
            logger.warn("Tried to lookup key from nonexistent filter. ColumnFamily: {}", columnFamily);

            return false;
        }

        return cfFilterMap.get(columnFamily).isPresent(key);
    }

    private IFilter createFilter() {
        int numOfElements = 1000;
        double falsePositiveRate = 0.01;

        return new CuckooFilter(numOfElements, falsePositiveRate);
    }

    private void markDirty() {
        lock.lock();
        try {
            dirty = true;
        } finally {
            lock.unlock();
        }
    }

    private GlobalFilterService() {
        // ignored
    }

    private void startFilterDirtyMonitor() {
        service.filterMonitor.start();
    }

    private void loadFiltersFromDisk() {
        Path globalFiltersPath = getGlobalFiltersPath();

        logger.debug("Global Filters Path: {}", globalFiltersPath.toAbsolutePath());

        if (globalFiltersPath.toFile().exists()) {
            try {
                byte[] filterBytes = Files.readAllBytes(globalFiltersPath);
                //noinspection unchecked
                tableFilters = (HashMap<String, HashMap<String, IFilter>>) SerializationUtils.deserialize(filterBytes);
            } catch (Exception e) {
                throw new RuntimeException("Cannot load Global Filter", e);
            }

            logger.debug("Loaded filters from disk");
        }
        else {
            logger.debug("Global Filter does not exist on disk.");
        }
    }

    private void saveFiltersToDisk() {
        byte[] bytes = SerializationUtils.serialize(tableFilters);

        try {
            Files.write(getGlobalFiltersPath(), bytes);
        } catch (Exception e) {
            throw new RuntimeException("Cannot save Global Filter", e);
        }

        logger.info("Saved filters to disk.");
    }

    private Path getGlobalFiltersPath() {
        String dataDir = DatabaseDescriptor.getAllDataFileLocations()[0];
        String storageDir = dataDir.substring(0, dataDir.length() - 5);

        return Paths.get(storageDir + '/' + GLOBAL_FILTER_FILE_NAME);
    }


    private class FilterMonitor extends Thread {
        @Override
        public void run() {
            logger.info("Starting Filter Monitor Service.");
            while (true) {
                try {
                    if (dirty) {
                        lock.lock();
                        try {
                            saveFiltersToDisk();
                            dirty = false;
                        } finally {
                            lock.unlock();
                        }
                    }

                    if (stopService) {
                        logger.info("Stopped Filter Monitor Service.");
                        break;
                    }

                    Thread.sleep(FILTER_DIRTY_MONITORING_INTERVAL_MILLIS);
                } catch (InterruptedException ignore) {
                }
            }
        }
    }
}
