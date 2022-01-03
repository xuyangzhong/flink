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

package org.apache.flink.streaming.api.operators.commoncollect;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.streaming.api.operators.collect.CollectSinkFunction;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A {@link CoordinationResponse} from the coordinator containing the required batch or new results
 * and other necessary information in serialized form.
 *
 * <p>For an explanation of this communication protocol, see Java docs in {@link
 * CollectSinkFunction}.
 */
public class CommonCollectCoordinationResponse implements CoordinationResponse {

    private static final long serialVersionUID = 1L;

    private static final TypeSerializer<Boolean> isOpenSerializer = BooleanSerializer.INSTANCE;
    private static final TypeSerializer<Long> batchSizeSerializer = LongSerializer.INSTANCE;
    private static final TypeSerializer<String> operatorIdSerializer = StringSerializer.INSTANCE;
    private static final TypeSerializer<Integer> subtaskIdSerializer = IntSerializer.INSTANCE;

    private static final ListSerializer<byte[]> bufferSerializer =
            new ListSerializer<>(BytePrimitiveArraySerializer.INSTANCE);

    private final boolean isOpen;
    private final long batchSize;
    private final String operatorId;
    private final int subtaskId;
    private final List<byte[]> serializedResults;

    public CommonCollectCoordinationResponse(
            boolean isOpen,
            long batchSize,
            String operatorId,
            int subtaskId,
            List<byte[]> serializedResults) {
        this.isOpen = isOpen;
        this.batchSize = batchSize;
        this.operatorId = operatorId;
        this.subtaskId = subtaskId;
        this.serializedResults = serializedResults;
    }

    public CommonCollectCoordinationResponse(DataInputView inView) throws IOException {
        this.isOpen = isOpenSerializer.deserialize(inView);
        this.batchSize = batchSizeSerializer.deserialize(inView);
        this.operatorId = operatorIdSerializer.deserialize(inView);
        this.subtaskId = subtaskIdSerializer.deserialize(inView);
        this.serializedResults = bufferSerializer.deserialize(inView);
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

    public <T> List<T> getResults(TypeSerializer<T> elementSerializer) throws IOException {
        List<T> results = new ArrayList<>();
        for (byte[] serializedResult : serializedResults) {
            ByteArrayInputStream bais = new ByteArrayInputStream(serializedResult);
            DataInputViewStreamWrapper wrapper = new DataInputViewStreamWrapper(bais);
            results.add(elementSerializer.deserialize(wrapper));
        }
        return results;
    }

    public void serialize(DataOutputView outView) throws IOException {
        isOpenSerializer.serialize(isOpen, outView);
        batchSizeSerializer.serialize(batchSize, outView);
        operatorIdSerializer.serialize(operatorId, outView);
        subtaskIdSerializer.serialize(subtaskId, outView);
        bufferSerializer.serialize(serializedResults, outView);
    }

    @Override
    public String toString() {
        return "CommonCollectCoordinationResponse{"
                + "isOpen="
                + isOpen
                + ", batchSize="
                + batchSize
                + ", operatorId='"
                + operatorId
                + '\''
                + ", subtaskId="
                + subtaskId
                + ", serializedResults="
                + serializedResults
                + '}';
    }
}
