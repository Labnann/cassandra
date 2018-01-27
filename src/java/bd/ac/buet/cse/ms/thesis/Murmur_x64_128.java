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

import java.nio.charset.Charset;

import com.google.common.hash.Funnel;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;

public class Murmur_x64_128 implements HashFunction {

    private long seed;

    public Murmur_x64_128(long seed) {
        this.seed = seed;
    }

    @Override
    public Hasher newHasher() {
        return null;
    }

    @Override
    public Hasher newHasher(int expectedInputSize) {
        return null;
    }

    @Override
    public HashCode hashInt(int input) {
        return null;
    }

    @Override
    public HashCode hashLong(long input) {
        return null;
    }

    @Override
    public HashCode hashBytes(byte[] input) {
        return null;
    }

    @Override
    public HashCode hashBytes(byte[] input, int off, int len) {
        return null;
    }

    @Override
    public HashCode hashUnencodedChars(CharSequence input) {
        return null;
    }

    @Override
    public HashCode hashString(CharSequence input, Charset charset) {
        return null;
    }

    @Override
    public <T> HashCode hashObject(T instance, Funnel<? super T> funnel) {
        return null;
    }

    @Override
    public int bits() {
        return 64;
    }
}
