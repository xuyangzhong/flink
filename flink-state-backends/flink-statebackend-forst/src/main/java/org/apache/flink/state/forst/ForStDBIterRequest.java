/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.forst;

import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.api.common.state.v2.StateIterator;
import org.apache.flink.core.state.InternalStateFuture;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.util.Preconditions;

import org.rocksdb.ColumnFamilyHandle;

import javax.annotation.Nonnull;

import java.io.IOException;

/**
 * The Iter access request for ForStDB.
 *
 * @param <T> The type of value in iterator returned by get request.
 */
public class ForStDBIterRequest<T> {

    /** The type of result returned by iterator. */
    public enum ResultType {
        KEY,
        VALUE,
        ENTRY,
    }

    private final ResultType resultType;

    /**
     * ContextKey that use to calculate prefix bytes. All entries under the same key have the same
     * prefix, hence we can stop iterating once coming across an entry with a different prefix.
     */
    @Nonnull private final ContextKey contextKey;

    /** The table that generated iter requests. */
    private final ForStMapState table;

    /**
     * The state request handler, used for {@link
     * org.apache.flink.runtime.asyncprocessing.StateRequestType#ITERATOR_LOADING}.
     */
    private final StateRequestHandler stateRequestHandler;

    private final InternalStateFuture<StateIterator<T>> future;

    private final int keyGroupPrefixBytes;

    /** The bytes to seek to. If null, seek start from the {@link #getKeyPrefixBytes}. */
    private final byte[] toSeekBytes;

    private Runnable disposer;

    public ForStDBIterRequest(
            ResultType type,
            ContextKey contextKey,
            ForStMapState table,
            StateRequestHandler stateRequestHandler,
            InternalStateFuture<StateIterator<T>> future,
            byte[] toSeekBytes,
            Runnable disposer) {
        this.resultType = type;
        this.contextKey = contextKey;
        this.table = table;
        this.stateRequestHandler = stateRequestHandler;
        this.future = future;
        this.keyGroupPrefixBytes = table.getKeyGroupPrefixBytes();
        this.toSeekBytes = toSeekBytes;
        this.disposer = disposer;
    }

    public int getKeyGroupPrefixBytes() {
        return keyGroupPrefixBytes;
    }

    public ResultType getResultType() {
        return resultType;
    }

    public State getState() {
        return table;
    }

    public StateRequestHandler getStateRequestHandler() {
        return stateRequestHandler;
    }

    public byte[] getKeyPrefixBytes() throws IOException {
        Preconditions.checkState(contextKey.getUserKey() == null);
        return table.serializeKey(contextKey);
    }

    public byte[] getToSeekBytes() {
        return toSeekBytes;
    }

    public ColumnFamilyHandle getColumnFamilyHandle() {
        return table.getColumnFamilyHandle();
    }

    public Object deserializeUserValue(byte[] valueBytes) throws IOException {
        Object userValue = table.deserializeValue(valueBytes);
        return userValue;
    }

    public Object deserializeUserKey(byte[] userKeyBytes, int userKeyOffset) throws IOException {
        Object userKey = table.deserializeUserKey(userKeyBytes, userKeyOffset);
        return userKey;
    }

    public void completeStateFuture(StateIterator<T> iterator) throws IOException {
        if (disposer != null) {
            disposer.run();
        }
        future.complete(iterator);
    }
}
