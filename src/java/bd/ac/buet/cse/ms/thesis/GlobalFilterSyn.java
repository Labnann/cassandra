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

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.HashMap;

import org.apache.commons.lang.SerializationUtils;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.IFilter;

public class GlobalFilterSyn implements Serializable {

    public static final IVersionedSerializer<GlobalFilterSyn> serializer = new GlobalFilterSynSerializer();

    private InetAddress endpoint;
    private HashMap<String, HashMap<String, IFilter>> filters;

    public GlobalFilterSyn(InetAddress endpoint, HashMap<String, HashMap<String, IFilter>> filters) {
        this.endpoint = endpoint;
        this.filters = filters;
    }

    public InetAddress getEndpoint() {
        return endpoint;
    }

    public HashMap<String, HashMap<String, IFilter>> getFilters() {
        return filters;
    }
}


class GlobalFilterSynSerializer implements IVersionedSerializer<GlobalFilterSyn> {

    public void serialize(GlobalFilterSyn digest, DataOutputPlus out, int version) throws IOException {
        byte[] bytes = SerializationUtils.serialize(digest);
        out.writeLong(bytes.length);
        out.write(bytes);
    }

    public GlobalFilterSyn deserialize(DataInputPlus in, int version) throws IOException {
        long size = in.readLong();
        byte[] bytes = new byte[(int) size];
        in.readFully(bytes);

        return (GlobalFilterSyn) SerializationUtils.deserialize(bytes);
    }

    public long serializedSize(GlobalFilterSyn digest, int version) {
        byte[] bytes = SerializationUtils.serialize(digest);

        return TypeSizes.sizeof(bytes.length) + bytes.length;
    }
}
