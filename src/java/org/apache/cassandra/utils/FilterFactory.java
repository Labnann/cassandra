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

import java.io.DataInput;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bd.ac.buet.cse.ms.thesis.CuckooFilter;
import bd.ac.buet.cse.ms.thesis.CuckooFilterSerializer;
import bd.ac.buet.cse.ms.thesis.FilterSwitch;
import org.apache.cassandra.db.transform.Filter;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.obs.IBitSet;
import org.apache.cassandra.utils.obs.OffHeapBitSet;
import org.apache.cassandra.utils.obs.OpenBitSet;

public class FilterFactory
{
    public static final IFilter AlwaysPresent = new AlwaysPresentFilter();

    private static final Logger logger = LoggerFactory.getLogger(FilterFactory.class);
    private static final long BITSET_EXCESS = 20;

    public static void serialize(IFilter bf, DataOutputPlus output) throws IOException
    {
        if (FilterSwitch.filter == FilterSwitch.CUCKOO_FILTER) {
            CuckooFilterSerializer.serialize((CuckooFilter) bf, output);
        } else {
            BloomFilterSerializer.serialize((BloomFilter) bf, output);
        }
    }

    public static IFilter deserialize(DataInput input, boolean offheap, boolean oldBfHashOrder) throws IOException
    {
        return FilterSwitch.filter == FilterSwitch.CUCKOO_FILTER
               ? CuckooFilterSerializer.deserialize(input)
               : BloomFilterSerializer.deserialize(input, offheap, oldBfHashOrder);
    }

    /**
     * @return A BloomFilter with the lowest practical false positive
     *         probability for the given number of elements.
     */
    public static IFilter getFilter(long numElements, int targetBucketsPerElem, boolean offheap, boolean oldBfHashOrder)
    {
        int maxBucketsPerElement = Math.max(1, BloomCalculations.maxBucketsPerElement(numElements));
        int bucketsPerElement = Math.min(targetBucketsPerElem, maxBucketsPerElement);
        if (bucketsPerElement < targetBucketsPerElem)
        {
            logger.warn("Cannot provide an optimal BloomFilter for {} elements ({}/{} buckets per element).", numElements, bucketsPerElement, targetBucketsPerElem);
        }
        BloomCalculations.BloomSpecification spec = BloomCalculations.computeBloomSpec(bucketsPerElement);
        return createFilter(spec.K, numElements, spec.bucketsPerElement, offheap, oldBfHashOrder);
    }

    /**
     * @return The smallest BloomFilter that can provide the given false
     *         positive probability rate for the given number of elements.
     *
     *         Asserts that the given probability can be satisfied using this
     *         filter.
     */
    public static IFilter getFilter(long numElements, double maxFalsePosProbability, boolean offheap, boolean oldBfHashOrder)
    {
        return getFilter(numElements, maxFalsePosProbability, offheap, oldBfHashOrder, false);
    }

    public static IFilter getFilter(long numElements, double maxFalsePosProbability, boolean offheap, boolean oldBfHashOrder, boolean globalFilter) {
        assert maxFalsePosProbability <= 1.0 : "Invalid probability";
        if (maxFalsePosProbability == 1.0)
            return new AlwaysPresentFilter();

//        logger.info("Creating filter. n={}", numElements);

        if ((globalFilter && FilterSwitch.globalFilter == FilterSwitch.CUCKOO_FILTER)
            || (!globalFilter && FilterSwitch.filter == FilterSwitch.CUCKOO_FILTER)) {
            CuckooFilter cuckooFilter = new CuckooFilter(Math.max(numElements, FilterSwitch.MIN_NO_OF_ELEMENTS_IN_CUCKOO_FILTER), maxFalsePosProbability);
//            logger.info("Cuckoo filter serialized size: {}", cuckooFilter.serializedSize());
            return cuckooFilter;
        }

        int bucketsPerElement = BloomCalculations.maxBucketsPerElement(numElements);
        BloomCalculations.BloomSpecification spec = BloomCalculations.computeBloomSpec(bucketsPerElement, maxFalsePosProbability);

//        logger.info("Creating bloom filter. K={}, n={}, bucketPerElem={}, storageSize={}, fpp={}", spec.K, numElements,
//                    spec.bucketsPerElement, (numElements * spec.bucketsPerElement) + BITSET_EXCESS, maxFalsePosProbability);

        IFilter bloomFilter = createFilter(spec.K, numElements, spec.bucketsPerElement, offheap, oldBfHashOrder);
//        logger.info("Bloom filter serialized size: {}", bloomFilter.serializedSize());
        return bloomFilter;
    }


    @SuppressWarnings("resource")
    private static IFilter createFilter(int hash, long numElements, int bucketsPer, boolean offheap, boolean oldBfHashOrder)
    {
        long numBits;
        switch (FilterSwitch.loadPercentage) {
            case 0:
                numBits = (long) (numElements * Math.pow(bucketsPer, 3f)) + BITSET_EXCESS;
                break;
            case 25:
                numBits = (long) (numElements * Math.pow(bucketsPer, 1.23f)) + BITSET_EXCESS;
                break;
            case 50:
                numBits = (long) (numElements * bucketsPer / 1.5f) + BITSET_EXCESS;
                break;
            case 75:
                numBits = (long) (numElements * bucketsPer / 3f) + BITSET_EXCESS;
                break;
            case 100:
                numBits = (long) (numElements * bucketsPer / 6f) + BITSET_EXCESS;
                break;
            default:
                numBits = (numElements * bucketsPer) + BITSET_EXCESS;
        }

        IBitSet bitset = new OpenBitSet(numBits);
        return new BloomFilter(hash, bitset, oldBfHashOrder);
    }
}
