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

package org.apache.flink.table.runtime.operators.aggregate.window.processors;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.runtime.generated.GeneratedNamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.generated.NamespaceAggsHandleFunctionBase;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.operators.window.groupwindow.assigners.GroupWindowAssigner;
import org.apache.flink.table.runtime.operators.window.groupwindow.context.AbstractTriggerContext;
import org.apache.flink.table.runtime.operators.window.groupwindow.context.AbstractWindowContext;
import org.apache.flink.table.runtime.operators.window.groupwindow.internal.MergingWindowProcessFunction;
import org.apache.flink.table.runtime.operators.window.groupwindow.triggers.EventTimeTriggers;
import org.apache.flink.table.runtime.operators.window.groupwindow.triggers.ProcessingTimeTriggers;
import org.apache.flink.table.runtime.operators.window.groupwindow.triggers.Trigger;
import org.apache.flink.table.runtime.operators.window.windowtvf.slicing.WindowTimerService;
import org.apache.flink.table.runtime.operators.window.windowtvf.unslicing.UnsliceAssigner;
import org.apache.flink.table.runtime.operators.window.windowtvf.unslicing.UnslicingWindowProcessor;
import org.apache.flink.table.runtime.operators.window.windowtvf.unslicing.UnslicingWindowTimerServiceImpl;
import org.apache.flink.table.runtime.util.TimeWindowUtil;

import javax.annotation.Nullable;

import java.time.ZoneId;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.table.runtime.util.TimeWindowUtil.toEpochMillsForTimer;

/**
 * A base implementation of {@link UnslicingWindowProcessor} for window aggregate.
 *
 * <p>TODO: support window buffer.
 */
public class AbstractUnsliceWindowAggProcessor extends AbstractWindowAggProcessor<TimeWindow>
        implements UnslicingWindowProcessor<TimeWindow> {
    protected final UnsliceAssigner<TimeWindow> unsliceAssigner;

    // ----------------------------------------------------------------------------------------

    private transient MetricGroup metrics;

    protected transient MergingWindowProcessFunction<RowData, TimeWindow> windowFunction;

    private transient TriggerContextImpl triggerContext;

    public AbstractUnsliceWindowAggProcessor(
            GeneratedNamespaceAggsHandleFunction<TimeWindow> genAggsHandler,
            UnsliceAssigner<TimeWindow> unsliceAssigner,
            TypeSerializer<RowData> accSerializer,
            int indexOfCountStar,
            ZoneId shiftTimeZone) {
        super(
                genAggsHandler,
                unsliceAssigner,
                accSerializer,
                unsliceAssigner.isEventTime(),
                indexOfCountStar,
                shiftTimeZone);
        this.unsliceAssigner = unsliceAssigner;
    }

    @Override
    public void open(Context<TimeWindow> context) throws Exception {
        super.open(context);
        this.metrics = context.getRuntimeContext().getMetricGroup();
        this.windowFunction =
                new MergingWindowProcessFunction<>(
                        unsliceAssigner.getInnerMergingWindowAssigner(),
                        aggregator,
                        unsliceAssigner
                                .getInnerMergingWindowAssigner()
                                .getWindowSerializer(new ExecutionConfig()),
                        // TODO support allow lateness
                        0L);

        // TODO support early / late fire, see more at FLINK-29692
        Trigger<TimeWindow> trigger;
        if (isEventTime) {
            trigger = EventTimeTriggers.afterEndOfWindow();
        } else {
            trigger = ProcessingTimeTriggers.afterEndOfWindow();
        }
        triggerContext =
                new TriggerContextImpl(
                        trigger, ctx.getTimerService(), shiftTimeZone, createWindowSerializer());
        triggerContext.open();

        WindowContextImpl windowContext =
                new WindowContextImpl(
                        shiftTimeZone,
                        ctx.getTimerService(),
                        internalWindowState,
                        // TODO support early / late fire to produce updates.
                        null,
                        aggregator,
                        unsliceAssigner.getInnerMergingWindowAssigner(),
                        triggerContext);

        this.windowFunction.open(windowContext);
    }

    @Override
    public boolean processElement(RowData key, RowData element) throws Exception {
        // the windows which the input row should be placed into
        Optional<TimeWindow> affectedWindowOp =
                unsliceAssigner.assignStateNamespace(element, clockService, windowFunction);
        boolean isElementDropped = true;
        if (affectedWindowOp.isPresent()) {
            TimeWindow affectedWindow = affectedWindowOp.get();
            isElementDropped = false;

            RowData acc = windowState.value(affectedWindow);
            if (acc == null) {
                acc = aggregator.createAccumulators();
            }
            aggregator.setAccumulators(affectedWindow, acc);

            if (RowDataUtil.isAccumulateMsg(element)) {
                aggregator.accumulate(element);
            } else {
                aggregator.retract(element);
            }
            acc = aggregator.getAccumulators();
            windowState.update(affectedWindow, acc);
        }

        // the actual window which the input row is belongs to
        Optional<TimeWindow> actualWindowOp =
                unsliceAssigner.assignActualWindow(element, clockService, windowFunction);
        if (actualWindowOp.isPresent()) {
            TimeWindow actualWindow = actualWindowOp.get();
            isElementDropped = false;
            triggerContext.setWindow(actualWindow);
            // register a timer for the window to fire and clean up
            // TODO support allowedLateness
            long triggerTime = toEpochMillsForTimer(actualWindow.maxTimestamp(), shiftTimeZone);
            if (isEventTime) {
                triggerContext.registerEventTimeTimer(triggerTime);
            } else {
                triggerContext.registerProcessingTimeTimer(triggerTime);
            }
        }
        return isElementDropped;
    }

    @Override
    public void fireWindow(long timerTimestamp, TimeWindow window) throws Exception {
        windowFunction.prepareAggregateAccumulatorForEmit(window);
        RowData aggResult = aggregator.getValue(window);
        triggerContext.setWindow(window);
        final boolean checkNeedFire;
        if (isEventTime) {
            checkNeedFire = triggerContext.onEventTime(timerTimestamp);
        } else {
            checkNeedFire = triggerContext.onProcessingTime(timerTimestamp);
        }
        // we shouldn't emit an empty window
        if (checkNeedFire && !emptySupplier.get()) {
            collect(aggResult);
        }
    }

    @Override
    public void clearWindow(long timerTimestamp, TimeWindow window) throws Exception {
        windowFunction.cleanWindowIfNeeded(window, timerTimestamp);
    }

    @Override
    public void advanceProgress(long progress) throws Exception {}

    @Override
    public void prepareCheckpoint() throws Exception {}

    @Override
    public TypeSerializer<TimeWindow> createWindowSerializer() {
        return unsliceAssigner
                .getInnerMergingWindowAssigner()
                .getWindowSerializer(new ExecutionConfig());
    }

    @Override
    protected WindowTimerService<TimeWindow> getWindowTimerService() {
        return new UnslicingWindowTimerServiceImpl(ctx.getTimerService(), shiftTimeZone);
    }

    private class WindowContextImpl extends AbstractWindowContext<RowData, TimeWindow> {

        public WindowContextImpl(
                ZoneId shiftTimeZone,
                InternalTimerService<TimeWindow> internalTimerService,
                InternalValueState<RowData, TimeWindow, RowData> windowState,
                @Nullable InternalValueState<RowData, TimeWindow, RowData> previousState,
                NamespaceAggsHandleFunctionBase<TimeWindow> windowAggregator,
                GroupWindowAssigner<TimeWindow> windowAssigner,
                AbstractTriggerContext<RowData, TimeWindow> triggerContext) {
            super(
                    shiftTimeZone,
                    internalTimerService,
                    windowState,
                    previousState,
                    windowAggregator,
                    windowAssigner,
                    triggerContext);
        }

        @Override
        protected long findCleanupTime(TimeWindow window) {
            return TimeWindowUtil.toEpochMillsForTimer(window.maxTimestamp(), shiftTimeZone);
        }

        @Override
        public <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor)
                throws Exception {
            requireNonNull(stateDescriptor, "The state properties must not be null");
            return ctx.getKeyedStateBackend()
                    .getPartitionedState(
                            VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            stateDescriptor);
        }

        @Override
        public RowData currentKey() {
            return ctx.getKeyedStateBackend().getCurrentKey();
        }
    }

    private class TriggerContextImpl extends AbstractTriggerContext<RowData, TimeWindow> {
        public TriggerContextImpl(
                Trigger<TimeWindow> trigger,
                InternalTimerService<TimeWindow> internalTimerService,
                ZoneId shiftTimeZone,
                TypeSerializer<TimeWindow> windowSerializer) {
            super(trigger, internalTimerService, shiftTimeZone, windowSerializer);
        }

        @Override
        protected <N, S extends State, T> S getOrCreateKeyedState(
                TypeSerializer<N> namespaceSerializer, StateDescriptor<S, T> stateDescriptor)
                throws Exception {
            return ctx.getKeyedStateBackend()
                    .getOrCreateKeyedState(namespaceSerializer, stateDescriptor);
        }

        @Override
        public MetricGroup getMetricGroup() {
            return metrics;
        }

        @Override
        public <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor) {
            try {
                return ctx.getKeyedStateBackend()
                        .getPartitionedState(
                                VoidNamespace.INSTANCE,
                                VoidNamespaceSerializer.INSTANCE,
                                stateDescriptor);
            } catch (Exception e) {
                throw new RuntimeException("Could not retrieve state", e);
            }
        }
    }
}
