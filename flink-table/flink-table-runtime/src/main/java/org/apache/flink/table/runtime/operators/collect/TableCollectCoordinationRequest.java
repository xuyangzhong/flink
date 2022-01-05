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
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;

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
    private static final TypeSerializer<Boolean> isOpenSerializer = BooleanSerializer.INSTANCE;
    private static final TypeSerializer<Boolean> isBatchSerializer = BooleanSerializer.INSTANCE;
    private static final TypeSerializer<Long> batchSizeSerializer = LongSerializer.INSTANCE;
    private static final TypeSerializer<String> operatorIdSerializer = StringSerializer.INSTANCE;
    private static final TypeSerializer<Integer> subtaskIdSerializer = IntSerializer.INSTANCE;

    private final long id;
    private final boolean isOpen;
    private final boolean isBounded;
    private final long batchSize;
    private final String operatorId;
    private final int subtaskId;

    public TableCollectCoordinationRequest(
            long id,
            boolean isOpen,
            boolean isBounded,
            long batchSize,
            String operatorId,
            int subtaskId) {
        this.id = id;
        this.isOpen = isOpen;
        this.batchSize = batchSize;
        this.operatorId = operatorId;
        this.subtaskId = subtaskId;
        this.isBounded = isBounded;
    }

    public TableCollectCoordinationRequest(DataInputView inView) throws IOException {
        this.id = idSerializer.deserialize(inView);
        this.isOpen = isOpenSerializer.deserialize(inView);
        this.isBounded = isBatchSerializer.deserialize(inView);
        this.batchSize = batchSizeSerializer.deserialize(inView);
        this.operatorId = operatorIdSerializer.deserialize(inView);
        this.subtaskId = subtaskIdSerializer.deserialize(inView);
    }

    public long getId() {
        return id;
    }

    public boolean isBounded() {
        return isBounded;
    }

    public boolean isOpen() {
        return isOpen;
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

    public void serialize(DataOutputView outView) throws IOException {
        idSerializer.serialize(id, outView);
        isOpenSerializer.serialize(isOpen, outView);
        isBatchSerializer.serialize(isBounded, outView);
        batchSizeSerializer.serialize(batchSize, outView);
        operatorIdSerializer.serialize(operatorId, outView);
        subtaskIdSerializer.serialize(subtaskId, outView);
    }

    @Override
    public String toString() {
        return "CollectCoordinationRequest{"
                + "id="
                + id
                + "isOpen="
                + isOpen
                + ", batchSize="
                + batchSize
                + ", operatorId='"
                + operatorId
                + '\''
                + ", subtaskId="
                + subtaskId
                + '}';
    }
}
