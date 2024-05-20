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

import org.apache.flink.runtime.asyncprocessing.StateRequestType;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The iterate operation implementation for ForStDB, which leverages rocksdb's iterator directly.
 */
public class ForStIterateOperation implements ForStDBOperation {

    private static final Logger LOG = LoggerFactory.getLogger(ForStIterateOperation.class);

    public static final int CACHE_SIZE_LIMIT = 128;

    private final RocksDB db;

    private final List<ForStDBIterRequest<?>> batchRequest;

    private final Executor executor;

    ForStIterateOperation(RocksDB db, List<ForStDBIterRequest<?>> batchRequest, Executor executor) {
        this.db = db;
        this.batchRequest = batchRequest;
        this.executor = executor;
    }

    @Override
    public CompletableFuture<Void> process() {
        CompletableFuture<Void> future = new CompletableFuture<>();

        AtomicInteger counter = new AtomicInteger(batchRequest.size());
        for (int i = 0; i < batchRequest.size(); i++) {
            ForStDBIterRequest<?> request = batchRequest.get(i);
            executor.execute(
                    () -> {
                        // todo: config read options
                        try (RocksIterator iter = db.newIterator(request.getColumnFamilyHandle())) {
                            byte[] prefix = request.getKeyPrefixBytes();
                            int userKeyOffset = prefix.length;
                            if (request.getToSeekBytes() != null) {
                                iter.seek(request.getToSeekBytes());
                            } else {
                                iter.seek(prefix);
                            }
                            List<RawEntry> entries = new ArrayList<>(CACHE_SIZE_LIMIT);
                            boolean encounterEnd = false;
                            byte[] nextSeek = prefix;
                            while (iter.isValid() && entries.size() < CACHE_SIZE_LIMIT) {
                                byte[] key = iter.key();
                                if (startWithKeyPrefix(
                                        prefix, key, request.getKeyGroupPrefixBytes())) {
                                    entries.add(new RawEntry(key, iter.value()));
                                    nextSeek = key;
                                } else {
                                    encounterEnd = true;
                                    break;
                                }
                                iter.next();
                            }
                            if (iter.isValid()) {
                                nextSeek = iter.key();
                            }
                            if (entries.size() < CACHE_SIZE_LIMIT) {
                                encounterEnd = true;
                            }
                            Collection<Object> deserializedEntries =
                                    new ArrayList<>(entries.size());
                            switch (request.getResultType()) {
                                case KEY:
                                    {
                                        for (RawEntry en : entries) {
                                            deserializedEntries.add(
                                                    request.deserializeUserKey(
                                                            en.rawKeyBytes, userKeyOffset));
                                        }
                                        break;
                                    }
                                case VALUE:
                                    {
                                        for (RawEntry en : entries) {
                                            deserializedEntries.add(
                                                    request.deserializeUserValue(en.rawValueBytes));
                                        }
                                        break;
                                    }
                                case ENTRY:
                                    {
                                        for (RawEntry en : entries) {
                                            deserializedEntries.add(
                                                    new MapEntry(
                                                            request.deserializeUserKey(
                                                                    en.rawKeyBytes, userKeyOffset),
                                                            request.deserializeUserValue(
                                                                    en.rawValueBytes)));
                                        }
                                        break;
                                    }
                                default:
                                    throw new IllegalStateException(
                                            "Unknown result type: " + request.getResultType());
                            }
                            ForStMapIterator stateIterator =
                                    new ForStMapIterator<>(
                                            request.getState(),
                                            request.getResultType(),
                                            StateRequestType.ITERATOR_LOADING,
                                            request.getStateRequestHandler(),
                                            deserializedEntries,
                                            encounterEnd,
                                            nextSeek);

                            request.completeStateFuture(stateIterator);
                        } catch (Exception e) {
                            LOG.warn("Error when process iterate operation for forStDB", e);
                            future.completeExceptionally(e);
                        } finally {
                            if (counter.decrementAndGet() == 0
                                    && !future.isCompletedExceptionally()) {
                                future.complete(null);
                            }
                        }
                    });
        }
        return future;
    }

    /**
     * Check if the raw key bytes start with the key prefix bytes.
     *
     * @param keyPrefixBytes the key prefix bytes.
     * @param rawKeyBytes the raw key bytes.
     * @param kgPrefixBytes the number of key group prefix bytes.
     * @return true if the raw key bytes start with the key prefix bytes.
     */
    public static boolean startWithKeyPrefix(
            byte[] keyPrefixBytes, byte[] rawKeyBytes, int kgPrefixBytes) {
        if (rawKeyBytes.length < keyPrefixBytes.length) {
            return false;
        }
        for (int i = keyPrefixBytes.length; --i >= kgPrefixBytes; ) {
            if (rawKeyBytes[i] != keyPrefixBytes[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * The map entry to store the serialized key and value.
     *
     * @param <K> The type of key.
     * @param <V> The type of value.
     */
    static class MapEntry<K, V> implements Map.Entry<K, V> {
        K key;
        V value;

        public MapEntry(K key, V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public V setValue(V value) {
            this.value = value;
            return value;
        }
    }

    /** The entry to store the raw key and value. */
    static class RawEntry {
        /**
         * The raw bytes of the key stored in RocksDB. Each user key is stored in RocksDB with the
         * format #KeyGroup#Key#Namespace#UserKey.
         */
        private byte[] rawKeyBytes;

        /** The raw bytes of the value stored in RocksDB. */
        private byte[] rawValueBytes;

        public RawEntry(byte[] rawKeyBytes, byte[] rawValueBytes) {
            this.rawKeyBytes = rawKeyBytes;
            this.rawValueBytes = rawValueBytes;
        }
    }
}
