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

package org.apache.flink.state.forst;

import org.apache.flink.runtime.asyncprocessing.StateExecutor;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestContainer;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.Executors;

import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * The {@link StateExecutor} implementation which executing batch {@link StateRequest}s for
 * ForStStateBackend.
 */
public class ForStStateExecutor implements StateExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(ForStStateExecutor.class);

    /**
     * The coordinator thread which schedules the execution of multiple batches of stateRequests.
     * The number of coordinator threads is 1 to ensure that multiple batches of stateRequests can
     * be executed sequentially.
     */
    private final ForStSingleThreadExecutor coordinatorThread;

    /** The worker thread that actually executes the {@link StateRequest}s. */
    private final ForStMultiThreadExecutor workerThreads;

    private final ForStMultiThreadExecutor writeThreads;

    private final Executor directExecutor = Executors.directExecutor();

    private final RocksDB db;

    private final WriteOptions writeOptions;

    public ForStStateExecutor(
            int ioParallelism, int writeThread, RocksDB db, WriteOptions writeOptions) {
        this.coordinatorThread =
                new ForStSingleThreadExecutor(new ExecutorThreadFactory("ForSt-Coordinator"));
        this.workerThreads =
                new ForStMultiThreadExecutor(
                        ioParallelism, new ExecutorThreadFactory("ForSt-worker-IO"));
        this.writeThreads =
                new ForStMultiThreadExecutor(
                        writeThread, new ExecutorThreadFactory("ForSt-write-IO"));
        this.db = db;
        this.writeOptions = writeOptions;
    }

    @Override
    public CompletableFuture<Void> executeBatchRequests(
            StateRequestContainer stateRequestContainer) {
        Preconditions.checkArgument(stateRequestContainer instanceof ForStStateRequestClassifier);
        ForStStateRequestClassifier stateRequestClassifier =
                (ForStStateRequestClassifier) stateRequestContainer;
        CompletableFuture<Void> resultFuture = new CompletableFuture<>();

        List<CompletableFuture<Void>> futures = new ArrayList<>(3);
        List<ForStDBPutRequest<?, ?>> putRequests = stateRequestClassifier.pollDbPutRequests();
        if (!putRequests.isEmpty()) {
            ForStWriteBatchOperation writeOperations =
                    new ForStWriteBatchOperation(db, putRequests, writeOptions, writeThreads);
            futures.add(writeOperations.process());
        }

        List<ForStDBGetRequest<?, ?>> getRequests = stateRequestClassifier.pollDbGetRequests();
        if (!getRequests.isEmpty()) {
            ForStGeneralMultiGetOperation getOperations =
                    new ForStGeneralMultiGetOperation(db, getRequests, workerThreads);
            futures.add(getOperations.process());
        }

        List<ForStDBIterRequest<?>> iterRequests = stateRequestClassifier.pollDbIterRequests();
        if (!iterRequests.isEmpty()) {
            ForStIterateOperation iterOperations =
                    new ForStIterateOperation(db, iterRequests, workerThreads);
            futures.add(iterOperations.process());
        }

        return resultFuture;
    }

    @Override
    public StateRequestContainer createStateRequestContainer(
            StateRequestHandler stateRequestHandler) {
        return new ForStStateRequestClassifier(stateRequestHandler);
    }

    @Override
    public void shutdown() {
        workerThreads.shutdown();
        coordinatorThread.shutdown();
        LOG.info("Shutting down the ForStStateExecutor.");
    }
}
