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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;

/** An interface for operators to enable to be collected outputs by client. */
public interface CommonCollectible<OUT> extends OperatorEventHandler {
    int BATCH_SIZE = 1;

    default CommonCollectSinkFunction<OUT> getCollectFunction(
            TypeSerializer<OUT> serializer, long batchSize, String operatorId) {
        return new CommonCollectSinkFunction<>(serializer, batchSize, operatorId);
    }

    default CommonCollectSinkFunction<OUT> getCollectFunction(
            TypeSerializer<OUT> serializer, String operatorId) {
        return new CommonCollectSinkFunction<>(serializer, BATCH_SIZE, operatorId);
    }

    void setOperatorEventGateway(OperatorEventGateway operatorEventGateway);

    void buildCollectFunction(String operatorId);

    void setSerializer(TypeSerializer<OUT> serializer);
}
