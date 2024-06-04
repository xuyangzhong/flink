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

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * The general-purpose multiGet operation implementation for ForStDB, which simulates multiGet by
 * calling the Get API multiple times with multiple threads.
 */
public class ForStGeneralMultiGetOperation implements ForStDBOperation {

    private static final Logger LOG = LoggerFactory.getLogger(ForStGeneralMultiGetOperation.class);

    private final RocksDB db;

    private final List<ForStDBGetRequest<?, ?>> batchRequest;

    private final Executor executor;

    ForStGeneralMultiGetOperation(
            RocksDB db, List<ForStDBGetRequest<?, ?>> batchRequest, Executor executor) {
        this.db = db;
        this.batchRequest = batchRequest;
        this.executor = executor;
    }

    @Override
    public CompletableFuture<Void> process() {
        return CompletableFuture.runAsync(
                () -> {
                    ReadOptions readOptions = new ReadOptions();
                    readOptions.setReadaheadSize(0);
                    List<byte[]> keys = new ArrayList<>(batchRequest.size());
                    List<ColumnFamilyHandle> columnFamilyHandles =
                            new ArrayList<>(batchRequest.size());
                    try {
                        for (int i = 0; i < batchRequest.size(); i++) {
                            ForStDBGetRequest<?, ?> request = batchRequest.get(i);
                            byte[] key = request.buildSerializedKey();
                            keys.add(key);
                            columnFamilyHandles.add(request.getColumnFamilyHandle());
                        }
                        List<byte[]> values =
                                db.multiGetAsList(readOptions, columnFamilyHandles, keys);
                        for (int i = 0; i < batchRequest.size(); i++) {
                            ForStDBGetRequest<?, ?> request = batchRequest.get(i);
                            request.completeStateFuture(values.get(i));
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                },
                executor);

        //        CompletableFuture<Void> future = new CompletableFuture<>();
        //
        //        AtomicInteger counter = new AtomicInteger(batchRequest.size());
        //
        //        for (int i = 0; i < batchRequest.size(); i++) {
        //            ForStDBGetRequest<?, ?> request = batchRequest.get(i);
        //            executor.execute(
        //                    () -> {
        //                        ReadOptions readOptions = new ReadOptions();
        //                        readOptions.setReadaheadSize(0);
        //                        RocksIterator iter = null;
        //                        try {
        //                            byte[] key = request.buildSerializedKey();
        //                            if (request.checkMapEmpty()) {
        //                                iter = db.newIterator(request.getColumnFamilyHandle(),
        // readOptions);
        //                                iter.seek(key);
        //                                if (iter.isValid()
        //                                        && startWithKeyPrefix(
        //                                                key,
        //                                                iter.key(),
        //                                                request.getKeyGroupPrefixBytes())) {
        //                                    request.completeStateFuture(new byte[0]);
        //                                } else {
        //                                    request.completeStateFuture(null);
        //                                }
        //                            } else {
        //                                byte[] value =
        //                                        db.get(request.getColumnFamilyHandle(),
        // readOptions, key);
        //                                request.completeStateFuture(value);
        //                            }
        //                        } catch (Exception e) {
        //                            LOG.warn(
        //                                    "Error when process general multiGet operation for
        // forStDB", e);
        //                            future.completeExceptionally(e);
        //                        } finally {
        //                            if (iter != null) {
        //                                iter.close();
        //                            }
        //                            if (counter.decrementAndGet() == 0
        //                                    && !future.isCompletedExceptionally()) {
        //                                future.complete(null);
        //                            }
        //                        }
        //                    });
        //        }
        //        return future;
    }
}
