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

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.IFilter;

public class GlobalFilterSyncService {

    private static final Logger logger = LoggerFactory.getLogger(GlobalFilterSyncService.class);

    private static final long GLOBAL_FILTER_SYNC_INTERVAL_MILLIS = 3 * 60 * 1000;

    private static GlobalFilterSyncService instance;

    private final SyncRunner syncRunner;

    public static void initialize() {
        if (!FilterSwitch.ENABLE_GLOBAL_FILTER || instance != null) {
            return;
        }

        instance = new GlobalFilterSyncService();
    }

    public static void destroy() {
        if (instance != null) {
            instance.stopService();
        }
    }

    public static void forceSyncNow() {
        if (instance != null) {
            instance.syncGlobalFilters();
        }
    }

    private void stopService() {
        syncRunner.stop = true;
        syncRunner.interrupt();
    }

    private GlobalFilterSyncService() {
        syncRunner = new SyncRunner();
        syncRunner.start();
    }

    private void syncGlobalFilters() {
        Set<InetAddress> liveNodes = Gossiper.instance.getLiveMembers();
        InetAddress thisNode = FBUtilities.getBroadcastAddress();
        liveNodes.remove(thisNode);    // remove this node

        if (liveNodes.isEmpty()) {
            return;
        }

        HashMap<String, HashMap<String, IFilter>> filters = GlobalFilterService.instance().getFilters(thisNode.toString());
        GlobalFilterSyn globalFilterSynMsg = new GlobalFilterSyn(thisNode, filters);

        MessageOut<GlobalFilterSyn> message = new MessageOut<>(MessagingService.Verb.GLOBAL_FILTER_SYN,
                                                               globalFilterSynMsg,
                                                               GlobalFilterSyn.serializer);

        for (InetAddress node : liveNodes) {
            EndpointState nodeState = Gossiper.instance.getEndpointStateForEndpoint(node);
            if (nodeState == null || !nodeState.isAlive() || !nodeState.isRpcReady()) {
                continue;
            }

            logger.info("Sending GlobalFilterSyn msg to {}", node.toString());
            MessagingService.instance().sendOneWay(message, node);
        }
    }


    private class SyncRunner extends Thread {

        private boolean stop = false;

        @Override
        public void run() {
            logger.info("Global Filter Sync service started.");

            while (true) {
                try {
                    if (stop) {
                        break;
                    }

                    syncGlobalFilters();

                    Thread.sleep(GLOBAL_FILTER_SYNC_INTERVAL_MILLIS);
                } catch (InterruptedException ignored) {
                }
            }

            logger.info("Global Filter Sync service stopped.");
        }
    }
}
