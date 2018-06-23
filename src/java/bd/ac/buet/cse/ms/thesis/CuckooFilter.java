/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package bd.ac.buet.cse.ms.thesis;

import java.io.Serializable;

import com.google.common.hash.Funnels;
import com.google.common.hash.HashCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.mgunlogson.cuckoofilter4j.Utils;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.concurrent.Ref;

public class CuckooFilter implements IFilter,Serializable {

    private static final Logger logger = LoggerFactory.getLogger(CuckooFilter.class);

    private final com.github.mgunlogson.cuckoofilter4j.CuckooFilter<byte[]> cuckooFilter;

    public CuckooFilter(long numOfElements, double falsePositiveRate) {
        cuckooFilter = new com.github.mgunlogson.cuckoofilter4j.CuckooFilter.Builder<>(Funnels.byteArrayFunnel(), numOfElements)
                       .withHashAlgorithm(Utils.Algorithm.Murmur3_x64_128)
                       .withFalsePositiveRate(falsePositiveRate)
                       .build();

//        logger.info("Initialized CuckooFilter. {}", this);
    }

    private CuckooFilter(CuckooFilter copy) {
        this.cuckooFilter = copy.cuckooFilter;

//        logger.info("Shared copied CuckooFilter. {}", this);
    }

    @Override
    public void add(FilterKey key) {
        HashCode[] hashCode = getHashCode(key);
        boolean successful = cuckooFilter.put(getItem(key), hashCode[0], hashCode[1]);

//        logger.info("CuckooFilter.add(); key={}; isSuccessful={}", key, successful);
    }

    @Override
    public void delete(FilterKey key) {
        HashCode[] hashCode = getHashCode(key);
        boolean successful = cuckooFilter.delete(getItem(key), hashCode[0], hashCode[1]);

//        logger.info("CuckooFilter.delete(); key={}; isSuccessful={}", key, successful);
    }

    private byte[] getItem(FilterKey key) {
        return ((DecoratedKey) key).getKey().array();
    }

    private HashCode[] getHashCode(FilterKey key) {
        long[] dest = new long[2];
        key.filterHash(dest);

//        logger.info("CuckooFilter.getHashCode(); key={}; hash={}", key, dest);

        return new HashCode[]{ HashCode.fromLong(dest[0]), HashCode.fromLong(dest[1]) };
    }

    @Override
    public boolean isPresent(FilterKey key) {
        HashCode[] hashCode = getHashCode(key);
        boolean present = cuckooFilter.mightContain(getItem(key), hashCode[0], hashCode[1]);

        if (FilterSwitch.LOG_LOOKUP_RESULTS) {
            logger.info("CuckooFilter.isPresent(); key={}; isPresent={}", key, present);
        }

        if (FilterSwitch.LOG_STATS) {
            logFilterStats();
        }

        return present;
    }

    private void logFilterStats() {
        int filterHashCode = System.identityHashCode(cuckooFilter);
        double loadFactor = cuckooFilter.getLoadFactor();
        long noOfItemsInFilter = cuckooFilter.getCount();
        long actualCapacity = cuckooFilter.getActualCapacity();
        long storageSize = cuckooFilter.getStorageSize();
        long serializedSize = serializedSize();

        logger.info("CuckooFilter.stats(); filterHashCode={}; loadFactor={}; noOfItemsInFilter={}; actualCapacity={}; storageSize={}; serializedSize={}",
                    filterHashCode, loadFactor, noOfItemsInFilter, actualCapacity, storageSize, serializedSize);
    }

    @Override
    public IFilter sharedCopy() {
        return new CuckooFilter(this);
    }

    @Override
    public long serializedSize() {
        return CuckooFilterSerializer.serializedSize(this);
    }

    @Override
    public long offHeapSize() {
        return 0;   //ignored
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("CuckooFilter.clear() not implemented!");
    }

    @Override
    public void addTo(Ref.IdentityCollection identities) {
        throw new UnsupportedOperationException("CuckooFilter.addTo() not implemented!");
    }

    @Override
    public void close() {
        //ignored
    }

    @Override
    public Throwable close(Throwable accumulate) {
        return null;    //ignored
    }

    public String toString() {
        return "CuckooFilter[falsePositiveProbability=" + cuckooFilter.getFalsePositiveProbability()
               + ";capacity=" + cuckooFilter.getActualCapacity()
               + ";storageSize=" + cuckooFilter.getStorageSize()
               + ";loadFactor=" + cuckooFilter.getLoadFactor()
               + ";count=" + cuckooFilter.getCount()
               + ";underlyingCuckooFilterHashCode=" + System.identityHashCode(cuckooFilter)
               + ";underlyingBitsHashCode=" + System.identityHashCode(cuckooFilter.table.memBlock.bits)
               + ']';
    }
}
