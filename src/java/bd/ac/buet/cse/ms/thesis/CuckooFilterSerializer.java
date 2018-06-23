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

import java.io.DataInput;
import java.io.IOException;

import org.apache.commons.lang.SerializationUtils;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataOutputPlus;

public final class CuckooFilterSerializer {

    private CuckooFilterSerializer() {
    }

    public static void serialize(CuckooFilter cuckooFilter, DataOutputPlus out) throws IOException {
        byte[] bytes = getSerializedFilterBytes(cuckooFilter);
        out.writeLong(bytes.length);
        out.write(bytes);
    }

    private static byte[] getSerializedFilterBytes(CuckooFilter cuckooFilter) {
        return SerializationUtils.serialize(cuckooFilter);
    }

    @SuppressWarnings({ "UncheckedCast", "unchecked" })
    public static CuckooFilter deserialize(DataInput in) throws IOException {
        long size = in.readLong();
        byte[] bytes = new byte[(int) size];
        in.readFully(bytes);

        return (CuckooFilter) SerializationUtils.deserialize(bytes);
    }

    /**
     * Calculates a serialized size of the given Bloom Filter
     *
     * @param cuckooFilter Bloom filter to calculate serialized size
     * @return serialized size of the given bloom filter
     * @see org.apache.cassandra.io.ISerializer#serialize(Object, DataOutputPlus)
     */
    public static long serializedSize(CuckooFilter cuckooFilter) {
        byte[] bytes = getSerializedFilterBytes(cuckooFilter);

        return TypeSizes.sizeof(bytes.length) + bytes.length;
    }
}
