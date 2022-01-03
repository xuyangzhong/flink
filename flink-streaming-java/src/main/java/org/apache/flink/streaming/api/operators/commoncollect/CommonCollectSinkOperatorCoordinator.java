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

package org.apache.flink.streaming.api.operators.commoncollect;

import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationRequestHandler;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.streaming.api.operators.collect.CollectSinkFunction;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperatorCoordinator;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Similar with {@link CollectSinkOperatorCoordinator}. However, this class is for more common
 * operators ant not just for {@link CollectSinkFunction}
 */
public class CommonCollectSinkOperatorCoordinator
        implements OperatorCoordinator, CoordinationRequestHandler {
    private static final Logger LOG =
            LoggerFactory.getLogger(CommonCollectSinkOperatorCoordinator.class);

    private static CommonCollectSinkOperatorCoordinator instance;

    private static final long DEFAULT_BATCH_SIZE = 10;

    private final int socketTimeout;

    // operator id -> operator parallel subtask id -> ip address and socket
    private Map<String, Map<Integer, OperatorIpAndSocket>> ipLists = new HashMap();

    private DataInputViewStreamWrapper inStream;
    private DataOutputViewStreamWrapper outStream;

    private ExecutorService executorService;

    private CommonCollectSinkOperatorCoordinator(int socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    @Override
    public void start() throws Exception {
        this.executorService =
                Executors.newSingleThreadExecutor(
                        new ExecutorThreadFactory(
                                "common-collect-operator-coordinator-executor-thread-pool"));
    }

    @Override
    public void close() throws Exception {
        closeAllConnection();
        this.executorService.shutdown();
    }

    @Override
    public void handleEventFromOperator(int subtask, OperatorEvent event) throws Exception {
        Preconditions.checkArgument(
                event instanceof CommonCollectSinkAddressEvent,
                "Operator event must be a CommonCollectSinkAddressEvent");
        String operatorId = ((CommonCollectSinkAddressEvent) event).getOperatorId();
        InetSocketAddress address = ((CommonCollectSinkAddressEvent) event).getAddress();

        LOG.info("Received operator id : " + operatorId + " : server address: " + address);
        if (ipLists == null || operatorId == null || operatorId.equals("")) {
            return;
        }

        Map<Integer, OperatorIpAndSocket> subtaskIdAndIpInfo =
                ipLists.getOrDefault(operatorId, new HashMap<>());
        subtaskIdAndIpInfo.put(subtask, new OperatorIpAndSocket(address));
        ipLists.put(operatorId, subtaskIdAndIpInfo);
    }

    @Override
    public CompletableFuture<CoordinationResponse> handleCoordinationRequest(
            CoordinationRequest request) {
        Preconditions.checkArgument(
                request instanceof CommonCollectCoordinationRequest,
                "Coordination request must be a CollectCoordinationRequest");

        CommonCollectCoordinationRequest collectRequest =
                (CommonCollectCoordinationRequest) request;
        CompletableFuture<CoordinationResponse> responseFuture = new CompletableFuture<>();

        if (ipLists == null || ipLists.size() == 0) {
            completeWithEmptyResponse(collectRequest, responseFuture, null, -1);
            return responseFuture;
        }

        executorService.submit(() -> handleRequestImpl(collectRequest, responseFuture));
        return responseFuture;
    }

    private void handleRequestImpl(
            CommonCollectCoordinationRequest request,
            CompletableFuture<CoordinationResponse> responseFuture) {
        String operatorId = request.getOperatorId();
        if (operatorId == null || operatorId.equals("")) {
            completeWithEmptyResponse(request, responseFuture, operatorId, -1);
            return;
        }

        int subtaskId = request.getSubtaskId();

        OperatorIpAndSocket ipAndSocket = ipLists.get(operatorId).get(subtaskId);

        if (ipAndSocket == null || ipAndSocket.address == null) {
            closeConnection(operatorId);
            LOG.info("ipAndSocket or address is null");
            completeWithEmptyResponse(request, responseFuture, operatorId, subtaskId);
            return;
        }

        InetSocketAddress address = ipAndSocket.address;
        Socket socket = ipAndSocket.socket;

        try {
            if (socket == null) {
                socket = new Socket();
                socket.setSoTimeout(socketTimeout);
                socket.setKeepAlive(true);
                socket.setTcpNoDelay(true);

                socket.connect(address);
                // set back
                ipAndSocket.socket = socket;

                inStream = new DataInputViewStreamWrapper(socket.getInputStream());
                outStream = new DataOutputViewStreamWrapper(socket.getOutputStream());
                LOG.info("Common collect sink connection established");
            }

            // send back to common collect sink server
            if (LOG.isDebugEnabled()) {
                LOG.debug("Forwarding request to common collect sink socket server");
            }
            request.serialize(outStream);

            // fetch back serialized results
            if (LOG.isDebugEnabled()) {
                LOG.debug("Fetching serialized result from common collect sink socket server");
            }
            responseFuture.complete(new CommonCollectCoordinationResponse(inStream));
        } catch (Exception e) {
            // request failed, close current connection and send back empty results
            // we catch every exception here because socket might suddenly be null if the sink
            // fails, but we do not want the coordinator to fail
            if (LOG.isDebugEnabled()) {
                // this is normal when sink restarts or job ends, so we print a debug log
                LOG.debug("Common collect sink coordinator encounters an exception", e);
            }
            closeConnection(operatorId);
            completeWithEmptyResponse(request, responseFuture, operatorId, subtaskId);
        }
    }

    private void completeWithEmptyResponse(
            CommonCollectCoordinationRequest request,
            CompletableFuture<CoordinationResponse> future,
            String operatorId,
            int subtaskId) {
        future.complete(
                new CommonCollectCoordinationResponse(
                        request.isOpen(),
                        DEFAULT_BATCH_SIZE,
                        operatorId,
                        subtaskId,
                        Collections.emptyList()));
    }

    private void closeAllConnection() {
        if (ipLists == null) {
            return;
        }
        for (String opId : ipLists.keySet()) {
            closeConnection(opId);
        }
    }

    private void closeConnection(String operatorId) {
        if (ipLists == null) {
            return;
        }
        if (ipLists.get(operatorId) == null) {
            return;
        }
        if (operatorId == null || operatorId.equals("")) {
            return;
        }

        for (int subtaskIds : ipLists.get(operatorId).keySet()) {
            Socket socket = ipLists.get(operatorId).get(subtaskIds).socket;
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    LOG.warn("Failed to close common collect sink socket server connection", e);
                }
            }
            ipLists.get(operatorId).get(subtaskIds).socket = null;
        }
    }

    @Override
    public void subtaskFailed(int subtask, @Nullable Throwable reason) {
        // subtask failed, the socket server does not exist anymore
    }

    @Override
    public void subtaskReset(int subtask, long checkpointId) {
        // nothing to do here, connections are re-created lazily
    }

    @Override
    public void subtaskReady(int subtask, OperatorCoordinator.SubtaskGateway gateway) {
        // nothing to do here, connections are re-created lazily
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result)
            throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(ipLists);

        result.complete(baos.toByteArray());
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    @Override
    public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData)
            throws Exception {
        if (checkpointData == null) {
            // restore before any checkpoint completed
            closeAllConnection();
        } else {
            ByteArrayInputStream bais = new ByteArrayInputStream(checkpointData);
            ObjectInputStream ois = new ObjectInputStream(bais);
            ipLists = (Map<String, Map<Integer, OperatorIpAndSocket>>) ois.readObject();
        }
    }

    /** Provider for {@link CollectSinkOperatorCoordinator}. */
    public static class Provider implements OperatorCoordinator.Provider {

        private final OperatorID operatorId;
        private final int socketTimeout;

        public Provider(OperatorID operatorId, int socketTimeout) {
            this.operatorId = operatorId;
            this.socketTimeout = socketTimeout;
        }

        @Override
        public OperatorID getOperatorId() {
            return operatorId;
        }

        @Override
        public OperatorCoordinator create(OperatorCoordinator.Context context) {
            // we do not send operator event so we don't need a context
            if (instance == null) {
                instance = new CommonCollectSinkOperatorCoordinator(socketTimeout);
            }
            return instance;
        }
    }

    static class OperatorIpAndSocket {
        public InetSocketAddress address;
        public Socket socket;

        public OperatorIpAndSocket(InetSocketAddress address) {
            this.address = address;
        }
    }
}
