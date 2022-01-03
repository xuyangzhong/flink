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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.operators.collect.CollectSinkFunction;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Similar with {@link CollectSinkFunction}, the difference is following:
 *
 * <p>This class can record different parallel subtask id to coordinator.
 *
 * <p>This class will not collect data until it gets the beginning signal from client.
 *
 * <p>This class will end collecting data until it gets the stopping signal from client.
 */
public class CommonCollectSinkFunction<IN> extends RichSinkFunction<IN>
        implements CheckpointedFunction, CheckpointListener {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(CommonCollectSinkFunction.class);

    private final TypeSerializer<IN> serializer;

    private transient long currentSize;

    //	private transient long batchSize;
    private final long batchSize;

    private final String operatorId;

    private transient OperatorEventGateway eventGateway;

    private transient LinkedList<byte[]> buffer;

    private transient ServerThread serverThread;

    private transient boolean isNeedToOutput = true;

    public CommonCollectSinkFunction(
            TypeSerializer<IN> serializer, long batchSize, String operatorId) {
        this.serializer = serializer;
        this.batchSize = batchSize;
        this.operatorId = operatorId;
    }

    private void initBuffer() {
        if (buffer != null) {
            return;
        }

        buffer = new LinkedList<>();
        currentSize = 0;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        initBuffer();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // do nothing
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        initBuffer();

        serverThread = new ServerThread(serializer);
        serverThread.start();

        // sending socket server address to coordinator
        Preconditions.checkNotNull(eventGateway, "Operator event gateway hasn't been set");
        InetSocketAddress address = serverThread.getServerSocketAddress();
        LOG.info("Common Collect sink server established, address = " + address);

        CommonCollectSinkAddressEvent addressEvent =
                new CommonCollectSinkAddressEvent(operatorId, address);
        eventGateway.sendEventToCoordinator(addressEvent);
    }

    @Override
    public void invoke(IN value) throws Exception {
        if (!isNeedToOutput) {
            return;
        }
        synchronized (buffer) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(baos);
            serializer.serialize(value, wrapper);

            buffer.add(baos.toByteArray());
            currentSize++;
        }
    }

    @Override
    public void close() throws Exception {
        serverThread.close();
        serverThread.join();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    @Override
    public void notifyCheckpointAborted(long checkpointId) {}

    public void setOperatorEventGateway(OperatorEventGateway eventGateway) {
        this.eventGateway = eventGateway;
    }

    /** The thread that runs the socket server. */
    private class ServerThread extends Thread {

        private final TypeSerializer<IN> serializer;
        private final ServerSocket serverSocket;

        private boolean running;

        private Socket connection;
        private DataInputViewStreamWrapper inStream;
        private DataOutputViewStreamWrapper outStream;

        private ServerThread(TypeSerializer<IN> serializer) throws Exception {
            this.serializer = serializer.duplicate();
            this.serverSocket = new ServerSocket(0, 0, getBindAddress());
            this.running = true;
            LOG.info("init server thread : " + serverSocket);
        }

        @Override
        public void run() {
            LOG.info("server thread : " + serverSocket + " running. " + " running : " + running);
            while (running) {
                try {
                    CommonCollectCoordinationRequest request = null;
                    if (connection == null) {
                        // waiting for coordinator to connect
                        connection = NetUtils.acceptWithoutTimeout(serverSocket);
                        inStream = new DataInputViewStreamWrapper(this.connection.getInputStream());

                        outStream =
                                new DataOutputViewStreamWrapper(this.connection.getOutputStream());
                        LOG.info("Coordinator connection received");
                    }
                    request = new CommonCollectCoordinationRequest(inStream);
                    LOG.info("server thread : " + serverSocket + "receive : " + request);
                    //                    CommonCollectCoordinationRequest request =
                    //                            new CommonCollectCoordinationRequest(inStream);

                    //                    if (request.isOpen()) {
                    //                        isNeedToOutput = true;
                    //                    } else {
                    //                        isNeedToOutput = false;
                    //                        buffer.clear();
                    //                        currentSize = 0;
                    //                        continue;
                    //                    }

                    //                    if (currentSize < batchSize) {
                    //                        continue;
                    //                    }
                    // valid request, sending out results
                    //                    long size = request.getBatchSize();

                    List<byte[]> nextBatch = new ArrayList<>();

                    synchronized (buffer) {
                        //                        long min = Math.min(currentSize, size);
                        //                        for (int i = 0; i < min; i++) {
                        //                            nextBatch.add(buffer.getFirst());
                        //                            buffer.removeFirst();
                        //                        }
                        nextBatch = new ArrayList<>(buffer);
                        buffer.clear();
                        //                        currentSize = currentSize - min;
                        currentSize = 0;
                    }

                    sendBackResults(nextBatch);
                } catch (Exception e) {
                    // Exception occurs, just close current connection
                    // client will come with the same offset if it needs the same batch of results
                    if (LOG.isDebugEnabled()) {
                        // this is normal when sink restarts or job ends, so we print a debug log
                        LOG.debug("Common Collect sink server encounters an exception", e);
                    }
                    closeCurrentConnection();
                    LOG.info("Common Collect sink server encounters an exception", e);
                }
            }
        }

        private void close() {
            LOG.info("common collect function closed");
            running = false;
            closeServerSocket();
            closeCurrentConnection();
        }

        private InetSocketAddress getServerSocketAddress() {
            RuntimeContext context = getRuntimeContext();
            Preconditions.checkState(
                    context instanceof StreamingRuntimeContext,
                    "CollectSinkFunction can only be used in StreamTask");
            StreamingRuntimeContext streamingContext = (StreamingRuntimeContext) context;
            String taskManagerAddress =
                    streamingContext.getTaskManagerRuntimeInfo().getTaskManagerExternalAddress();
            return new InetSocketAddress(taskManagerAddress, serverSocket.getLocalPort());
        }

        private InetAddress getBindAddress() {
            //            RuntimeContext context = getRuntimeContext();
            //            Preconditions.checkState(
            //                    context instanceof StreamingRuntimeContext,
            //                    "CollectSinkFunction can only be used in StreamTask");
            //            StreamingRuntimeContext streamingContext = (StreamingRuntimeContext)
            // context;
            //            String taskManagerAddress =
            //
            // streamingContext.getTaskManagerRuntimeInfo().getTaskManagerExternalAddress();
            //            return new InetSocketAddress(taskManagerAddress, 0).getAddress();
            RuntimeContext context = getRuntimeContext();
            Preconditions.checkState(
                    context instanceof StreamingRuntimeContext,
                    "CollectSinkFunction can only be used in StreamTask");
            StreamingRuntimeContext streamingContext = (StreamingRuntimeContext) context;
            String bindAddress =
                    streamingContext.getTaskManagerRuntimeInfo().getTaskManagerBindAddress();

            if (bindAddress != null) {
                try {
                    return InetAddress.getByName(bindAddress);
                } catch (UnknownHostException e) {
                    return null;
                }
            }
            return null;
        }

        private void sendBackResults(List<byte[]> serializedResults) throws IOException {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Sending back " + serializedResults.size() + " results");
            }
            CommonCollectCoordinationResponse response =
                    new CommonCollectCoordinationResponse(
                            running,
                            batchSize,
                            operatorId,
                            getRuntimeContext().getIndexOfThisSubtask(),
                            serializedResults);
            response.serialize(outStream);
            LOG.info("server thread : " + serverSocket + " send out : " + response);
        }

        private void closeCurrentConnection() {
            try {
                if (connection != null) {
                    connection.close();
                    connection = null;
                }
            } catch (Exception e) {
                LOG.warn(
                        "Error occurs when closing client connections in CommonCollectSinkFunction",
                        e);
            }
        }

        private void closeServerSocket() {
            try {
                serverSocket.close();
            } catch (Exception e) {
                LOG.warn("Error occurs when closing server in CommonCollectSinkFunction", e);
            }
        }
    }
}
