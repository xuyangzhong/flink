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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.state.InternalStateFuture;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Map;

/**
 * The Bunch Put access request for ForStDB.
 *
 * @param <K> The type of key in original state access request.
 */
public class ForStDBBunchPutRequest<K> extends ForStDBPutRequest<ContextKey<K>, Map<?, ?>> {

    /** Serializer for the user values. */
    final TypeSerializer<Object> userValueSerializer;

    /** The data outputStream used for value serializer, which should be thread-safe. */
    final ThreadLocal<DataOutputSerializer> valueSerializerView;

    /** The data inputStream used for value deserializer, which should be thread-safe. */
    final ThreadLocal<DataInputDeserializer> valueDeserializerView;

    public ForStDBBunchPutRequest(
            ContextKey<K> key,
            Map value,
            ForStMapState table,
            InternalStateFuture<Void> future,
            Runnable disposer) {
        super(key, value, table, future, disposer);
        Preconditions.checkArgument(table instanceof ForStMapState);
        this.userValueSerializer = table.userValueSerializer;
        this.valueSerializerView = table.valueSerializerView;
        this.valueDeserializerView = table.valueDeserializerView;
    }

    public Map<?, ?> getBunchValue() {
        return value;
    }

    @Override
    public byte[] buildSerializedKey() throws IOException {
        key.resetExtra();
        return table.serializeKey(key);
    }

    public byte[] buildSerializedKey(Object userKey) throws IOException {
        key.setUserKey(userKey);
        return table.serializeKey(key);
    }

    public byte[] buildSerializedValue(Object singleValue) throws IOException {
        DataOutputSerializer outputView = valueSerializerView.get();
        outputView.clear();
        userValueSerializer.serialize(singleValue, outputView);
        return outputView.getCopyOfBuffer();
    }

    /**
     * Find the next byte array that is lexicographically larger than input byte array.
     *
     * @param bytes the input byte array.
     * @return the next byte array.
     */
    public static byte[] nextBytes(byte[] bytes) {
        Preconditions.checkState(bytes != null && bytes.length > 0);
        int len = bytes.length;
        byte[] nextBytes = new byte[len];
        System.arraycopy(bytes, 0, nextBytes, 0, len);
        boolean find = false;
        for (int i = len - 1; i >= 0; i--) {
            byte currentByte = ++nextBytes[i];
            if (currentByte != Byte.MIN_VALUE) {
                find = true;
                break;
            }
        }
        if (!find) {
            byte[] newBytes = new byte[len + 1];
            System.arraycopy(bytes, 0, newBytes, 0, len);
            newBytes[len] = 1;
            return newBytes;
        }
        return nextBytes;
    }
}
