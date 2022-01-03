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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.commoncollect.CommonCollectSinkFunction;
import org.apache.flink.streaming.api.operators.commoncollect.CommonCollectible;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.util.functions.StreamingFunctionUtils;

/** A {@link StreamOperator} for executing {@link MapFunction MapFunctions}. */
@Internal
public class StreamMap<IN, OUT> extends AbstractUdfStreamOperator<OUT, MapFunction<IN, OUT>>
        implements OneInputStreamOperator<IN, OUT>, CommonCollectible<OUT> {

    private static final long serialVersionUID = 1L;
    //
    private CommonCollectSinkFunction<OUT> collectFunction;
    //
    private boolean enableCollection = true;
    //
    private TypeSerializer<OUT> serializer;

    public StreamMap(MapFunction<IN, OUT> mapper) {
        super(mapper);
        chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        OUT newRecord = userFunction.map(element.getValue());
        output.collect(element.replace(newRecord));
        if (enableCollection) {
            collectFunction.invoke(newRecord);
        }
    }

    @Override
    public void setSerializer(TypeSerializer<OUT> serializer) {
        this.serializer = serializer;
    }

    @Override
    public void setOperatorEventGateway(OperatorEventGateway operatorEventGateway) {
        collectFunction.setOperatorEventGateway(operatorEventGateway);
    }

    @Override
    public void handleOperatorEvent(OperatorEvent evt) {
        // do nothing
    }

    @Override
    public void buildCollectFunction(String operatorId) {
        this.collectFunction = new CommonCollectSinkFunction<>(serializer, 1, operatorId);
    }

    @Override
    public void open() throws Exception {
        super.open();
        FunctionUtils.openFunction(collectFunction, new Configuration());
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<OUT>> output) {
        super.setup(containingTask, config, output);

        //        final Environment environment = containingTask.getEnvironment();
        //        InternalOperatorMetricGroup operatorMetricGroup =
        //                environment
        //                        .getMetricGroup()
        //                        .getOrAddOperator(config.getOperatorID(),
        // config.getOperatorName());
        //        this.output =
        //                new CommonCollectOutput<>(
        //                        output,
        //                        operatorMetricGroup.getIOMetricGroup().getNumRecordsOutCounter(),
        //                        collectFunction);
        FunctionUtils.setFunctionRuntimeContext(collectFunction, getRuntimeContext());
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        StreamingFunctionUtils.snapshotFunctionState(
                context, getOperatorStateBackend(), collectFunction);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        StreamingFunctionUtils.restoreFunctionState(context, collectFunction);
    }

    @Override
    public void finish() throws Exception {
        super.finish();
        collectFunction.finish();
    }
}
