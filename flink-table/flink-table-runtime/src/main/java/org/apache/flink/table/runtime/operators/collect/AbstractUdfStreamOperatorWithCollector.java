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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.util.functions.StreamingFunctionUtils;
import org.apache.flink.util.function.RunnableWithException;

/**
 * This is used as the base class for operators that have a user-defined function. This class
 * handles the opening and closing of the user-defined functions, as part of the operator life
 * cycle.
 *
 * @param <OUT> The output type of the operator
 * @param <F> The type of the user function
 */
@PublicEvolving
public abstract class AbstractUdfStreamOperatorWithCollector<OUT, F extends Function>
        extends AbstractUdfStreamOperator<OUT, F> implements Collectible<OUT> {

    private MailboxExecutor executor;

    private TableCollectSinkFunction<OUT> collectFunction;

    private TypeSerializer<OUT> serializer;

    public AbstractUdfStreamOperatorWithCollector(F userFunction) {
        super(userFunction);
    }

    @Override
    public void setOperatorEventGateway(OperatorEventGateway operatorEventGateway) {
        collectFunction.setOperatorEventGateway(operatorEventGateway);
    }

    @Override
    public void buildCollectFunction(String operatorId, MailboxExecutor executor) {
        this.collectFunction = new TableCollectSinkFunction<>(serializer, 1, operatorId);
        this.executor = executor;
    }

    @Override
    public void setSerializer(TypeSerializer<OUT> serializer) {
        this.serializer = serializer;
    }

    public void startConsume(long id) {
        executor.submit(
                new RunnableWithException() {
                    @Override
                    public void run() throws Exception {
                        if (AbstractUdfStreamOperatorWithCollector.this instanceof Scannable) {
                            ((Scannable) AbstractUdfStreamOperatorWithCollector.this)
                                    .scan(collectFunction, id);
                        }
                    }
                },
                "subscribe");
    }

    // ------------------------------------------------------------------------
    //  operator life cycle
    // ------------------------------------------------------------------------

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<OUT>> output) {
        super.setup(containingTask, config, output);
        // rewrite
        FunctionUtils.setFunctionRuntimeContext(collectFunction, getRuntimeContext());
        this.output = new TableCollectOutput<>(this.output, collectFunction);

        FunctionUtils.setFunctionRuntimeContext(userFunction, getRuntimeContext());

        FunctionUtils.setFunctionRuntimeContext(collectFunction, getRuntimeContext());
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        StreamingFunctionUtils.snapshotFunctionState(
                context, getOperatorStateBackend(), userFunction);
        collectFunction.snapshotState(context);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        StreamingFunctionUtils.restoreFunctionState(context, userFunction);
        collectFunction.initializeState(context);
    }

    @Override
    public void open() throws Exception {
        super.open();
        FunctionUtils.openFunction(userFunction, new Configuration());
        collectFunction.open(new Configuration());
    }

    @Override
    public void finish() throws Exception {
        super.finish();
        if (userFunction instanceof SinkFunction) {
            ((SinkFunction<?>) userFunction).finish();
        }
        collectFunction.finish();
    }

    @Override
    public void close() throws Exception {
        super.close();
        FunctionUtils.closeFunction(userFunction);
        FunctionUtils.closeFunction(collectFunction);
    }

    // ------------------------------------------------------------------------
    //  checkpointing and recovery
    // ------------------------------------------------------------------------

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);

        if (userFunction instanceof CheckpointListener) {
            ((CheckpointListener) userFunction).notifyCheckpointComplete(checkpointId);
        }
        collectFunction.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        super.notifyCheckpointAborted(checkpointId);

        if (userFunction instanceof CheckpointListener) {
            ((CheckpointListener) userFunction).notifyCheckpointAborted(checkpointId);
        }
        collectFunction.notifyCheckpointAborted(checkpointId);
    }
}
