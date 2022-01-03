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
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

import java.time.Duration;

/**
 * The Factory for operator that implements {@link CommonCollectible}.
 *
 * @param <OUT>
 */
public class CommonCollectOperatorFactory<OUT> extends SimpleOperatorFactory<Object>
        implements CoordinatedOperatorFactory<Object> {

    private static final long serialVersionUID = 1L;

    private final AbstractStreamOperator operator;

    private final int socketTimeoutMillis;

    public CommonCollectOperatorFactory(
            TypeSerializer<OUT> serializer,
            Duration socketTimeout,
            AbstractStreamOperator<OUT> operator) {
        super((StreamOperator<Object>) operator);
        assert getOperator() instanceof CommonCollectible;
        this.operator = (AbstractStreamOperator<OUT>) getOperator();
        ((CommonCollectible<Object>) operator).setSerializer((TypeSerializer<Object>) serializer);
        this.socketTimeoutMillis = (int) socketTimeout.toMillis();
    }

    public CommonCollectOperatorFactory(
            TypeSerializer serializer, AbstractStreamOperator<OUT> operator) {
        this(serializer, Duration.ofSeconds(999999), operator);
    }

    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(
            String operatorName, OperatorID operatorID) {
        return new CommonCollectSinkOperatorCoordinator.Provider(operatorID, socketTimeoutMillis);
    }

    @Override
    public <T extends StreamOperator<Object>> T createStreamOperator(
            StreamOperatorParameters<Object> parameters) {
        final OperatorID operatorId = parameters.getStreamConfig().getOperatorID();
        final OperatorEventDispatcher eventDispatcher = parameters.getOperatorEventDispatcher();

        ((CommonCollectible<Object>) operator).buildCollectFunction(operatorId.toString());
        ((CommonCollectible<Object>) operator)
                .setOperatorEventGateway(eventDispatcher.getOperatorEventGateway(operatorId));
        operator.setProcessingTimeService(processingTimeService);
        operator.setup(
                parameters.getContainingTask(),
                parameters.getStreamConfig(),
                parameters.getOutput());

        eventDispatcher.registerEventHandler(operatorId, (OperatorEventHandler) operator);

        return (T) operator;
    }
}
