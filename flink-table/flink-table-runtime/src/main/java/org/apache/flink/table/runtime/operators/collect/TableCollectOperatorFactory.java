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

package org.apache.flink.table.runtime.operators.collect;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.YieldingOperatorFactory;

import java.time.Duration;

/**
 * The Factory for operator that implements {@link Collectible}.
 *
 * @param <OUT>
 */
public class TableCollectOperatorFactory<OUT> extends AbstractStreamOperatorFactory<OUT>
        implements YieldingOperatorFactory<OUT>, CoordinatedOperatorFactory<OUT> {

    private static final long serialVersionUID = 1L;

    private final AbstractUdfStreamOperatorWithCollector<OUT, ?> operator;

    private final int socketTimeoutMillis;

    public TableCollectOperatorFactory(
            TypeSerializer<OUT> serializer,
            Duration socketTimeout,
            AbstractUdfStreamOperatorWithCollector<OUT, ?> operator) {
        this.operator = operator;
        ((Collectible<OUT>) operator).setSerializer(serializer);
        this.socketTimeoutMillis = (int) socketTimeout.toMillis();
    }

    public TableCollectOperatorFactory(
            TypeSerializer serializer, AbstractUdfStreamOperatorWithCollector<OUT, ?> operator) {
        this(serializer, Duration.ofSeconds(999999), operator);
    }

    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(
            String operatorName, OperatorID operatorID) {
        return new TableCollectSinkOperatorCoordinator.Provider(operatorID, socketTimeoutMillis);
    }

    @Override
    public <T extends StreamOperator<OUT>> T createStreamOperator(
            StreamOperatorParameters<OUT> parameters) {
        final OperatorID operatorId = parameters.getStreamConfig().getOperatorID();

        operator.buildCollectFunction(operatorId.toString(), getMailboxExecutor());
        operator.setProcessingTimeService(processingTimeService);
        operator.setup(
                parameters.getContainingTask(),
                parameters.getStreamConfig(),
                parameters.getOutput());

        final OperatorEventDispatcher eventDispatcher = parameters.getOperatorEventDispatcher();
        operator.setOperatorEventGateway(eventDispatcher.getOperatorEventGateway(operatorId));
        eventDispatcher.registerEventHandler(operatorId, operator);

        return (T) operator;
    }

    @Override
    public void setChainingStrategy(ChainingStrategy strategy) {
        operator.setChainingStrategy(strategy);
    }

    @Override
    public ChainingStrategy getChainingStrategy() {
        return operator.getChainingStrategy();
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return operator.getClass();
    }
}
