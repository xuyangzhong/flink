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

package org.apache.flink.table.runtime.operators.window.windowtvf.operator;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.dataview.PerWindowStateDataViewStore;
import org.apache.flink.table.runtime.dataview.StateDataViewStore;
import org.apache.flink.table.runtime.generated.NamespaceAggsHandleFunctionBase;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.operators.window.groupwindow.assigners.GroupWindowAssigner;
import org.apache.flink.table.runtime.operators.window.groupwindow.assigners.MergingWindowAssigner;
import org.apache.flink.table.runtime.operators.window.groupwindow.context.AbstractTriggerContext;
import org.apache.flink.table.runtime.operators.window.groupwindow.context.WindowContext;
import org.apache.flink.table.runtime.operators.window.groupwindow.internal.MergingWindowProcessFunction;
import org.apache.flink.table.runtime.operators.window.groupwindow.triggers.EventTimeTriggers;
import org.apache.flink.table.runtime.operators.window.groupwindow.triggers.ProcessingTimeTriggers;
import org.apache.flink.table.runtime.operators.window.groupwindow.triggers.Trigger;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.TimeWindowUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.function.BiConsumerWithException;

import java.time.ZoneId;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.table.runtime.util.TimeWindowUtil.toEpochMillsForTimer;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The operator for unaligned window table function.
 *
 * <p>See more details about aligned window and unaligned window in {@link
 * org.apache.flink.table.runtime.operators.window.windowtvf.common.AbstractWindowOperator}.
 *
 * <p>Note: The operator only applies for Window TVF with set semantics (e.g SESSION) instead of row
 * semantics (e.g TUMBLE/HOP/CUMULATE).
 *
 * <p>This operator emits result at the end of window instead of per record.
 *
 * <p>This operator will not compact changelog records.
 *
 * <p>This operator will keep the original order of input records when outputting.
 */
public class UnalignedWindowTableFunctionOperator extends WindowTableFunctionOperatorBase
        implements Triggerable<RowData, TimeWindow> {

    private static final long serialVersionUID = 1L;

    private static final String LATE_ELEMENTS_DROPPED_METRIC_NAME = "numLateRecordsDropped";
    private static final String LATE_ELEMENTS_DROPPED_RATE_METRIC_NAME = "lateRecordsDroppedRate";
    private static final String WATERMARK_LATENCY_METRIC_NAME = "watermarkLatency";

    private final Trigger<TimeWindow> trigger;

    private final LogicalType[] inputFieldTypes;

    private final TypeSerializer<TimeWindow> windowSerializer;

    private transient InternalTimerService<TimeWindow> internalTimerService;

    // a counter to tag the order of all input streams when entering the operator
    private transient ValueState<Long> counterState;

    private transient InternalMapState<RowData, TimeWindow, Long, RowData> windowState;

    private transient TriggerContextImpl triggerContext;

    protected transient MergingWindowProcessFunction<RowData, TimeWindow> windowFunction;

    protected transient NamespaceAggsHandleFunctionBase<TimeWindow> windowAggregator;

    // ------------------------------------------------------------------------
    // Metrics
    // ------------------------------------------------------------------------

    private transient Counter numLateRecordsDropped;
    private transient Meter lateRecordsDroppedRate;
    private transient Gauge<Long> watermarkLatency;

    public UnalignedWindowTableFunctionOperator(
            GroupWindowAssigner<TimeWindow> windowAssigner,
            TypeSerializer<TimeWindow> windowSerializer,
            LogicalType[] inputFieldTypes,
            int rowtimeIndex,
            ZoneId shiftTimeZone) {
        super(windowAssigner, rowtimeIndex, shiftTimeZone);
        this.trigger = createTrigger(windowAssigner);
        this.windowSerializer = checkNotNull(windowSerializer);
        this.inputFieldTypes = checkNotNull(inputFieldTypes);
    }

    @Override
    public void open() throws Exception {
        super.open();

        internalTimerService =
                getInternalTimerService("window-table-functions", windowSerializer, this);

        triggerContext =
                new TriggerContextImpl(
                        trigger, internalTimerService, shiftTimeZone, windowSerializer);
        triggerContext.open();

        TypeInformation<RowData> inputRowType = InternalTypeInfo.ofFields(inputFieldTypes);

        ValueStateDescriptor<Long> counterStateDescriptor =
                new ValueStateDescriptor<>("window-table-counter", Types.LONG, 0L);
        counterState = getRuntimeContext().getState(counterStateDescriptor);

        MapStateDescriptor<Long, RowData> windowStateDescriptor =
                new MapStateDescriptor<>("window-table-accs", Types.LONG, inputRowType);

        windowState =
                (InternalMapState<RowData, TimeWindow, Long, RowData>)
                        getOrCreateKeyedState(windowSerializer, windowStateDescriptor);

        windowAggregator = new DummyWindowAggregator();
        windowAggregator.open(
                new PerWindowStateDataViewStore(
                        getKeyedStateBackend(), windowSerializer, getRuntimeContext()));

        WindowContextImpl windowContext = new WindowContextImpl();

        windowFunction =
                new MergingWindowProcessFunction<>(
                        (MergingWindowAssigner<TimeWindow>) windowAssigner,
                        windowAggregator,
                        windowSerializer,
                        0,
                        new MergingConsumer());

        windowFunction.open(windowContext);

        // metrics
        numLateRecordsDropped = metrics.counter(LATE_ELEMENTS_DROPPED_METRIC_NAME);
        lateRecordsDroppedRate =
                metrics.meter(
                        LATE_ELEMENTS_DROPPED_RATE_METRIC_NAME,
                        new MeterView(numLateRecordsDropped));
        watermarkLatency =
                metrics.gauge(
                        WATERMARK_LATENCY_METRIC_NAME,
                        () -> {
                            long watermark = internalTimerService.currentWatermark();
                            if (watermark < 0) {
                                return 0L;
                            } else {
                                return internalTimerService.currentProcessingTime() - watermark;
                            }
                        });
    }

    @Override
    public void close() throws Exception {
        if (windowAggregator != null) {
            windowAggregator.close();
        }
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        RowData inputRow = element.getValue();
        // no matter if order exceeds the Long.MAX_VALUE
        long order = counterState.value();
        counterState.update(order + 1);

        long timestamp;
        if (windowAssigner.isEventTime()) {
            if (inputRow.isNullAt(rowtimeIndex)) {
                // null timestamp would be dropped
                return;
            }
            timestamp = inputRow.getTimestamp(rowtimeIndex, 3).getMillisecond();
        } else {
            timestamp = getProcessingTimeService().getCurrentProcessingTime();
        }

        timestamp = TimeWindowUtil.toUtcTimestampMills(timestamp, shiftTimeZone);
        // the windows which the input row should be placed into
        Collection<TimeWindow> affectedWindows =
                windowFunction.assignStateNamespace(inputRow, timestamp);
        boolean isElementDropped = true;
        for (TimeWindow window : affectedWindows) {
            isElementDropped = false;

            windowState.setCurrentNamespace(window);
            windowState.put(order, inputRow);
        }

        // the actual window which the input row is belongs to
        Collection<TimeWindow> actualWindows =
                windowFunction.assignActualWindows(inputRow, timestamp);
        for (TimeWindow window : actualWindows) {
            isElementDropped = false;

            triggerContext.setWindow(window);
            boolean triggerResult = triggerContext.onElement(inputRow, timestamp);
            if (triggerResult) {
                emitWindowResult(window);
            }
            // clear up state
            registerCleanupTimer(window);
        }
        if (isElementDropped) {
            // markEvent will increase numLateRecordsDropped
            lateRecordsDroppedRate.markEvent();
        }
    }

    private void registerCleanupTimer(TimeWindow window) {
        long cleanupTime = getCleanupTime(window);
        if (cleanupTime == Long.MAX_VALUE) {
            // no need to clean up because we didn't set one
            return;
        }
        if (windowAssigner.isEventTime()) {
            triggerContext.registerEventTimeTimer(cleanupTime);
        } else {
            triggerContext.registerProcessingTimeTimer(cleanupTime);
        }
    }

    private void emitWindowResult(TimeWindow window) throws Exception {
        TimeWindow stateWindow = windowFunction.getStateWindow(window);
        windowState.setCurrentNamespace(stateWindow);
        Iterator<Map.Entry<Long, RowData>> iterator = windowState.iterator();
        // build a sorted map
        TreeMap<Long, RowData> sortedMap = new TreeMap<>();
        while (iterator.hasNext()) {
            Map.Entry<Long, RowData> entry = iterator.next();
            sortedMap.put(entry.getKey(), entry.getValue());
        }
        // emit the sorted map
        for (Map.Entry<Long, RowData> entry : sortedMap.entrySet()) {
            collect(entry.getValue(), Collections.singletonList(window));
        }
    }

    @Override
    public void onEventTime(InternalTimer<RowData, TimeWindow> timer) throws Exception {
        triggerContext.setWindow(timer.getNamespace());
        if (triggerContext.onEventTime(timer.getTimestamp())) {
            // fire
            emitWindowResult(triggerContext.getWindow());
        }

        if (windowAssigner.isEventTime()) {
            windowFunction.cleanWindowIfNeeded(triggerContext.getWindow(), timer.getTimestamp());
        }
    }

    @Override
    public void onProcessingTime(InternalTimer<RowData, TimeWindow> timer) throws Exception {
        triggerContext.setWindow(timer.getNamespace());
        if (triggerContext.onProcessingTime(timer.getTimestamp())) {
            // fire
            emitWindowResult(triggerContext.getWindow());
        }

        if (!windowAssigner.isEventTime()) {
            windowFunction.cleanWindowIfNeeded(triggerContext.getWindow(), timer.getTimestamp());
        }
    }

    /**
     * In case this leads to a value greated than {@link Long#MAX_VALUE} then a cleanup time of
     * {@link Long#MAX_VALUE} is returned.
     */
    private long getCleanupTime(TimeWindow window) {
        // In case this leads to a value greater than Long.MAX_VALUE, then a cleanup
        // time of Long.MAX_VALUE is returned.
        long cleanupTime = Math.max(0, window.maxTimestamp());
        cleanupTime = cleanupTime >= window.maxTimestamp() ? cleanupTime : Long.MAX_VALUE;
        return toEpochMillsForTimer(cleanupTime, shiftTimeZone);
    }

    private static Trigger<TimeWindow> createTrigger(
            GroupWindowAssigner<TimeWindow> windowAssigner) {
        if (windowAssigner.isEventTime()) {
            return EventTimeTriggers.afterEndOfWindow();
        } else {
            return ProcessingTimeTriggers.afterEndOfWindow();
        }
    }

    private class WindowContextImpl implements WindowContext<RowData, TimeWindow> {

        @Override
        public void deleteCleanupTimer(TimeWindow window) throws Exception {
            long cleanupTime = UnalignedWindowTableFunctionOperator.this.getCleanupTime(window);
            if (cleanupTime == Long.MAX_VALUE) {
                // no need to clean up because we didn't set one
                return;
            }
            if (windowAssigner.isEventTime()) {
                triggerContext.deleteEventTimeTimer(cleanupTime);
            } else {
                triggerContext.deleteProcessingTimeTimer(cleanupTime);
            }
        }

        @Override
        public <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor)
                throws Exception {
            requireNonNull(stateDescriptor, "The state properties must not be null");
            return UnalignedWindowTableFunctionOperator.this.getPartitionedState(stateDescriptor);
        }

        @Override
        public RowData currentKey() {
            return (RowData) UnalignedWindowTableFunctionOperator.this.getCurrentKey();
        }

        @Override
        public long currentProcessingTime() {
            return internalTimerService.currentProcessingTime();
        }

        @Override
        public long currentWatermark() {
            return internalTimerService.currentWatermark();
        }

        @Override
        public ZoneId getShiftTimeZone() {
            return shiftTimeZone;
        }

        @Override
        public void clearWindowState(TimeWindow window) throws Exception {
            windowState.setCurrentNamespace(window);
            windowState.clear();
        }

        @Override
        public void clearTrigger(TimeWindow window) throws Exception {
            triggerContext.setWindow(window);
            triggerContext.clear();
        }

        @Override
        public void onMerge(TimeWindow newWindow, Collection<TimeWindow> mergedWindows)
                throws Exception {
            triggerContext.setWindow(newWindow);
            triggerContext.setMergedWindows(mergedWindows);
            triggerContext.onMerge();
        }

        // ====================== ignored methods =====================

        @Override
        public void clearPreviousState(TimeWindow window) throws Exception {}

        @Override
        public RowData getWindowAccumulators(TimeWindow window) throws Exception {
            return null;
        }

        @Override
        public void setWindowAccumulators(TimeWindow window, RowData acc) throws Exception {}
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
            return UnalignedWindowTableFunctionOperator.this.getOrCreateKeyedState(
                    namespaceSerializer, stateDescriptor);
        }

        @Override
        public MetricGroup getMetricGroup() {
            return UnalignedWindowTableFunctionOperator.this.getMetricGroup();
        }

        @Override
        public <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor) {
            try {
                return UnalignedWindowTableFunctionOperator.this.getPartitionedState(
                        window, windowSerializer, stateDescriptor);
            } catch (Exception e) {
                throw new RuntimeException("Could not retrieve state", e);
            }
        }
    }

    private class MergingConsumer
            implements BiConsumerWithException<TimeWindow, Collection<TimeWindow>, Exception> {

        @Override
        public void accept(
                TimeWindow stateWindowResult, Collection<TimeWindow> stateWindowsToBeMerged)
                throws Exception {
            for (TimeWindow mergedWindow : stateWindowsToBeMerged) {
                windowState.setCurrentNamespace(mergedWindow);
                Iterator<Map.Entry<Long, RowData>> iterator = windowState.iterator();
                windowState.setCurrentNamespace(stateWindowResult);
                while (iterator.hasNext()) {
                    Map.Entry<Long, RowData> entry = iterator.next();
                    windowState.put(entry.getKey(), entry.getValue());
                }
            }
        }
    }

    /**
     * Use a dummy window aggregator to reuse the same logic in legacy group session window
     * operator.
     */
    private static class DummyWindowAggregator
            implements NamespaceAggsHandleFunctionBase<TimeWindow> {

        private final IllegalStateException thrown =
                new IllegalStateException(
                        "The function should not be called in DummyWindowAggregator");

        @Override
        public void open(StateDataViewStore store) throws Exception {}

        @Override
        public void setAccumulators(TimeWindow namespace, RowData accumulators) throws Exception {
            throw thrown;
        }

        @Override
        public void accumulate(RowData inputRow) throws Exception {
            throw thrown;
        }

        @Override
        public void retract(RowData inputRow) throws Exception {
            throw thrown;
        }

        @Override
        public void merge(TimeWindow namespace, RowData otherAcc) throws Exception {
            throw thrown;
        }

        @Override
        public RowData createAccumulators() throws Exception {
            throw thrown;
        }

        @Override
        public RowData getAccumulators() throws Exception {
            throw thrown;
        }

        @Override
        public void cleanup(TimeWindow namespace) throws Exception {
            throw thrown;
        }

        @Override
        public void close() throws Exception {}
    }
}
