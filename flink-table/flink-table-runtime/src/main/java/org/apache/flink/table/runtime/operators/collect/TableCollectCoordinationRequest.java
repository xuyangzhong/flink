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

package org.apache.flink.table.runtime.operators.collect;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * A {@link CoordinationRequest} from the client indicating that it wants a new batch of query
 * results.
 *
 * <p>For an explanation of this communication protocol, see Java docs in {@link
 * TableCollectSinkFunction}.
 */
public class TableCollectCoordinationRequest implements CoordinationRequest {

    private static final long serialVersionUID = 1L;

    private static final TypeSerializer<Long> idSerializer = LongSerializer.INSTANCE;
    private static final TypeSerializer<Boolean> isCanceledSerializer = BooleanSerializer.INSTANCE;
    private static final TypeSerializer<Boolean> isBoundedSerializer = BooleanSerializer.INSTANCE;
    private static final TypeSerializer<Long> batchSizeSerializer = LongSerializer.INSTANCE;
    private static final TypeSerializer<String> operatorIdSerializer = StringSerializer.INSTANCE;
    private static final TypeSerializer<Boolean> hasKeySerializer = BooleanSerializer.INSTANCE;
    private static final TypeSerializer<Integer> subtaskIdSerializer = IntSerializer.INSTANCE;
    private static final TypeSerializer<String> keyTypeSerializer = StringSerializer.INSTANCE;
    private static final TypeSerializer<byte[]> keySerializer =
            BytePrimitiveArraySerializer.INSTANCE;

    private final long id;
    private final boolean isCanceled;
    private final boolean isBounded;
    private final long batchSize;
    private final String operatorId;
    private final int subtaskId;
    private final @Nullable byte[] key;
    private final @Nullable RowType keyType;

    public TableCollectCoordinationRequest(
            long id,
            boolean isCanceled,
            boolean isBounded,
            long batchSize,
            String operatorId,
            int subtaskId,
            @Nullable byte[] key,
            @Nullable RowType keyType) {
        this.id = id;
        this.isCanceled = isCanceled;
        this.batchSize = batchSize;
        this.operatorId = operatorId;
        this.subtaskId = subtaskId;
        this.isBounded = isBounded;
        this.key = key;
        this.keyType = keyType;
    }

    public TableCollectCoordinationRequest(DataInputView inView) throws IOException {
        this.id = idSerializer.deserialize(inView);
        this.isCanceled = isCanceledSerializer.deserialize(inView);
        this.isBounded = isBoundedSerializer.deserialize(inView);
        this.batchSize = batchSizeSerializer.deserialize(inView);
        this.operatorId = operatorIdSerializer.deserialize(inView);
        this.subtaskId = subtaskIdSerializer.deserialize(inView);
        boolean hasKey = hasKeySerializer.deserialize(inView);
        if (hasKey) {
            String rowTypeStr = keyTypeSerializer.deserialize(inView);
            // TODO improve it
            keyType = (RowType) LogicalTypeParser.parse(rowTypeStr);
            this.key = keySerializer.deserialize(inView);
        } else {
            this.key = null;
            this.keyType = null;
        }
    }

    public long getId() {
        return id;
    }

    public boolean isBounded() {
        return isBounded;
    }

    public boolean isCanceled() {
        return isCanceled;
    }

    public long getBatchSize() {
        return batchSize;
    }

    public String getOperatorId() {
        return operatorId;
    }

    public int getSubtaskId() {
        return subtaskId;
    }

    @Nullable
    RowType getKeyType() {
        return keyType;
    }

    @Nullable
    public byte[] getKey() {
        return key;
    }

    public void serialize(DataOutputView outView) throws IOException {
        idSerializer.serialize(id, outView);
        isCanceledSerializer.serialize(isCanceled, outView);
        isBoundedSerializer.serialize(isBounded, outView);
        batchSizeSerializer.serialize(batchSize, outView);
        operatorIdSerializer.serialize(operatorId, outView);
        subtaskIdSerializer.serialize(subtaskId, outView);
        if (key != null && keyType != null) {
            hasKeySerializer.serialize(true, outView);
            keyTypeSerializer.serialize(keyType.asSerializableString(), outView);
            keySerializer.serialize(key, outView);
        } else {
            hasKeySerializer.serialize(false, outView);
        }
    }

    @Override
    public String toString() {
        return "CollectCoordinationRequest{"
                + "id="
                + id
                + "isCanceled="
                + isCanceled
                + "isBounded="
                + isBounded
                + ", batchSize="
                + batchSize
                + ", operatorId='"
                + operatorId
                + '\''
                + ", subtaskId="
                + subtaskId
                + ", hasKey="
                + (key != null)
                + '}';
    }
}
