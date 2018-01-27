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

import com.google.common.hash.Funnels;
import com.google.common.hash.HashCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.mgunlogson.cuckoofilter4j.Utils;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.concurrent.Ref;

public class CuckooFilter implements IFilter {

    private static final Logger logger = LoggerFactory.getLogger(CuckooFilter.class);

    private final com.github.mgunlogson.cuckoofilter4j.CuckooFilter<byte[]> cuckooFilter;

    public CuckooFilter(long numOfElements, double falsePositiveRate) {
        cuckooFilter = new com.github.mgunlogson.cuckoofilter4j.CuckooFilter.Builder<>(Funnels.byteArrayFunnel(), numOfElements)
                       .withHashAlgorithm(Utils.Algorithm.xxHash64)
                       .withFalsePositiveRate(falsePositiveRate)
                       .build();

        logger.info("Initialized CuckooFilter. {}", this);
    }

    CuckooFilter(com.github.mgunlogson.cuckoofilter4j.CuckooFilter<byte[]> underlyingCuckooFilter) {
        this.cuckooFilter = underlyingCuckooFilter;

        logger.info("Shared copied CuckooFilter. {}", this);
    }

    public com.github.mgunlogson.cuckoofilter4j.CuckooFilter<byte[]> getUnderlyingCuckooFilter() {
        return cuckooFilter;
    }

    @Override
    public void add(FilterKey key) {
        boolean successful = cuckooFilter.put(getItem(key), getHashCode(key));

        logger.info("CuckooFilter.add(); key={}; hash0={}; isSuccessful={}", key, getHashCode(key).asLong(), successful);
    }

    private byte[] getItem(FilterKey key) {
        return ((DecoratedKey) key).getKey().array();
    }

    private HashCode getHashCode(FilterKey key) {
        long[] dest = new long[2];
        key.filterHash(dest);

//        logger.info("CuckooFilter.getHashCode(); key={}; hash={}", key, dest);

        return HashCode.fromLong(dest[0]);
    }

    @Override
    public boolean isPresent(FilterKey key) {
        boolean present = cuckooFilter.mightContain(getItem(key), getHashCode(key));

        logger.info("CuckooFilter.isPresent(); key={}; isPresent={}", key, present);

        return present;
    }

    @Override
    public IFilter sharedCopy() {
        return new CuckooFilter(cuckooFilter);
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
