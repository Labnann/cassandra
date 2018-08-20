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
package org.apache.cassandra.utils;

import java.io.Serializable;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bd.ac.buet.cse.ms.thesis.CuckooFilter;
import bd.ac.buet.cse.ms.thesis.FilterSwitch;
import io.netty.util.concurrent.FastThreadLocal;
import net.nicoulaj.compilecommand.annotations.Inline;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.WrappedSharedCloseable;
import org.apache.cassandra.utils.obs.IBitSet;
import org.apache.cassandra.utils.obs.OffHeapBitSet;
import org.apache.cassandra.utils.obs.OpenBitSet;

public class BloomFilter extends WrappedSharedCloseable implements IFilter, Serializable
{
    private static final Logger logger = LoggerFactory.getLogger(BloomFilter.class);

    private final static FastThreadLocal<long[]> reusableIndexes = new FastThreadLocal<long[]>()
    {
        protected long[] initialValue()
        {
            return new long[21];
        }
    };

    public IBitSet bitset;
    public int hashCount;
    /**
     * CASSANDRA-8413: 3.0 (inverted) bloom filters have no 'static' bits caused by using the same upper bits
     * for both bloom filter and token distribution.
     */
    public boolean oldBfHashOrder;

    BloomFilter() {
    }

    public void restoreInstantiation() {
        super.setWrapped(bitset);
    }

    BloomFilter(int hashCount, IBitSet bitset, boolean oldBfHashOrder)
    {
        super(bitset);
        this.hashCount = hashCount;
        this.bitset = bitset;
        this.oldBfHashOrder = oldBfHashOrder;

//        logger.info("Initialized BloomFilter. {}", this);
    }

    private BloomFilter(BloomFilter copy)
    {
        super(copy);
        this.hashCount = copy.hashCount;
        this.bitset = copy.bitset;
        this.oldBfHashOrder = copy.oldBfHashOrder;

//        logger.info("Shared copy BloomFilter. {}", this);
    }

    public long serializedSize()
    {
        return BloomFilterSerializer.serializedSize(this);
    }

    // Murmur is faster than an SHA-based approach and provides as-good collision
    // resistance.  The combinatorial generation approach described in
    // http://www.eecs.harvard.edu/~kirsch/pubs/bbbf/esa06.pdf
    // does prove to work in actual tests, and is obviously faster
    // than performing further iterations of murmur.

    // tests ask for ridiculous numbers of hashes so here is a special case for them
    // rather than using the threadLocal like we do in production
    @VisibleForTesting
    public long[] getHashBuckets(FilterKey key, int hashCount, long max)
    {
        long[] hash = new long[2];
        key.filterHash(hash);
        long[] indexes = new long[hashCount];
        setIndexes(hash[1], hash[0], hashCount, max, indexes);
        return indexes;
    }

    // note that this method uses the threadLocal that may be longer than hashCount
    // to avoid generating a lot of garbage since stack allocation currently does not support stores
    // (CASSANDRA-6609).  it returns the array so that the caller does not need to perform
    // a second threadlocal lookup.
    @Inline
    private long[] indexes(FilterKey key)
    {
        // we use the same array both for storing the hash result, and for storing the indexes we return,
        // so that we do not need to allocate two arrays.
        long[] indexes = reusableIndexes.get();

        key.filterHash(indexes);
        setIndexes(indexes[1], indexes[0], hashCount, bitset.capacity(), indexes);
        return indexes;
    }

    @Inline
    private void setIndexes(long base, long inc, int count, long max, long[] results)
    {
        if (oldBfHashOrder)
        {
            long x = inc;
            inc = base;
            base = x;
        }

        for (int i = 0; i < count; i++)
        {
            results[i] = FBUtilities.abs(base % max);
            base += inc;
        }
    }

    public void add(FilterKey key)
    {
//        logger.info("BloomFilter.add(); key={}", key);

        long[] indexes = indexes(key);
        for (int i = 0; i < hashCount; i++)
        {
            bitset.set(indexes[i]);
        }
    }

    public final boolean isPresent(FilterKey key)
    {
        boolean present = true;
        long[] indexes = indexes(key);
        for (int i = 0; i < hashCount; i++)
        {
            if (!bitset.get(indexes[i]))
            {
                present = false;
                break;
            }
        }

        if (FilterSwitch.LOG_LOOKUP_RESULTS) {
            logger.info("BloomFilter.isPresent(); key={}; isPresent={}", key, present);
        }

        if (FilterSwitch.LOG_STATS) {
            logFilterStats();
        }

        return present;
    }

    @Override
    public void delete(FilterKey key) {
        //no-op
    }

    private void logFilterStats() {
        OpenBitSet openBitSet;
        OffHeapBitSet offHeapBitSet;
        try {
            openBitSet = (OpenBitSet) bitset;

            int filterHashCode = System.identityHashCode(openBitSet);
            long noOfItemsInFilter = 0;
            for (int i = 0; i < openBitSet.size(); i++) {
                if (openBitSet.get(i)) {
                    noOfItemsInFilter++;
                }
            }
            long actualCapacity = openBitSet.capacity();
            long storageSize = openBitSet.size();
            long serializedSize = openBitSet.serializedSize();
            double loadFactor = (double) noOfItemsInFilter / (double) storageSize;

            logger.info("BloomFilter.stats() [openbitset]; filterHashCode={}; loadFactor={}; noOfItemsInFilter={}; actualCapacity={}; storageSize={}; serializedSize={}",
                        filterHashCode, loadFactor, noOfItemsInFilter, actualCapacity, storageSize, serializedSize);
        } catch (ClassCastException e) {
            offHeapBitSet = (OffHeapBitSet) bitset;

            int filterHashCode = System.identityHashCode(offHeapBitSet);
            long noOfItemsInFilter = 0;
            for (int i = 0; i < offHeapBitSet.offHeapSize(); i++) {
                if (offHeapBitSet.get(i)) {
                    noOfItemsInFilter++;
                }
            }
            long actualCapacity = offHeapBitSet.capacity();
            long storageSize = offHeapBitSet.offHeapSize();
            long serializedSize = offHeapBitSet.serializedSize();
            double loadFactor = (double) noOfItemsInFilter / actualCapacity;

            logger.info("BloomFilter.stats() [offheapbitset]; filterHashCode={}; loadFactor={}; noOfItemsInFilter={}; actualCapacity={}; storageSize={}; serializedSize={}",
                        filterHashCode, loadFactor, noOfItemsInFilter, actualCapacity, storageSize, serializedSize);
        }
    }

    public void clear()
    {
        bitset.clear();
    }

    public IFilter sharedCopy()
    {
        return new BloomFilter(this);
    }

    @Override
    public long offHeapSize()
    {
        return bitset.offHeapSize();
    }

    public String toString()
    {
        return "BloomFilter[hashCount=" + hashCount + ";oldBfHashOrder=" + oldBfHashOrder + ";capacity=" + bitset.capacity() + ";hashCode=" + System.identityHashCode(bitset) + ']';
    }

    public void addTo(Ref.IdentityCollection identities)
    {
        super.addTo(identities);
        bitset.addTo(identities);
    }
}
