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

public class FilterSwitch {

    public static final int BLOOM_FILTER = 0;
    public static final int CUCKOO_FILTER = 1;

    public static final int MIN_NO_OF_ELEMENTS_IN_CUCKOO_FILTER = 4;

    public static final boolean LOG_STATS = false;
    public static final boolean LOG_LOOKUP_RESULTS = false;
    public static final boolean ENABLE_CUCKOO_DELETION = true;
    public static final boolean ENABLE_GLOBAL_FILTER = false;

    public static final int loadPercentage = -1;    // -1 = default; other possible values: 0, 25, 50, 75, 100
    public static final int filter = BLOOM_FILTER;
    public static final int globalFilter = BLOOM_FILTER;

}
