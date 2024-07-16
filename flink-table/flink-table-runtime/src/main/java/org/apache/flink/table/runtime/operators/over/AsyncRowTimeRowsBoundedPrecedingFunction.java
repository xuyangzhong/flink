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

package org.apache.flink.table.runtime.operators.over;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.v2.MapState;
import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.asyncprocessing.declare.DeclarationContext;
import org.apache.flink.runtime.asyncprocessing.declare.DeclarationException;
import org.apache.flink.runtime.asyncprocessing.declare.DeclaredVariable;
import org.apache.flink.runtime.asyncprocessing.declare.NamedBiFunction;
import org.apache.flink.runtime.asyncprocessing.declare.NamedFunction;
import org.apache.flink.runtime.state.v2.MapStateDescriptor;
import org.apache.flink.runtime.state.v2.ValueStateDescriptor;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.ThrowingConsumer;
import org.apache.flink.util.function.TriConsumerWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Async process Function for ROWS clause event-time bounded OVER window.
 *
 * <p>E.g.: SELECT rowtime, b, c, min(c) OVER (PARTITION BY b ORDER BY rowtime ROWS BETWEEN 2
 * PRECEDING AND CURRENT ROW), max(c) OVER (PARTITION BY b ORDER BY rowtime ROWS BETWEEN 2 PRECEDING
 * AND CURRENT ROW) FROM T.
 */
public class AsyncRowTimeRowsBoundedPrecedingFunction<K>
        extends KeyedProcessFunction<K, RowData, RowData> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG =
            LoggerFactory.getLogger(AsyncRowTimeRowsBoundedPrecedingFunction.class);

    private final GeneratedAggsHandleFunction genAggsHandler;
    private final LogicalType[] accTypes;
    private final LogicalType[] inputFieldTypes;
    private final long precedingOffset;
    private final int rowTimeIdx;

    protected transient JoinedRowData output;

    // the state which keeps the last triggering timestamp
    private transient ValueState<Long> lastTriggeringTsState;

    // the state which keeps the count of data
    private transient ValueState<Long> counterState;

    // the state which used to materialize the accumulator for incremental calculation
    private transient ValueState<RowData> accState;

    // the state which keeps all the data that are not expired.
    // The first element (as the mapState key) of the tuple is the time stamp. Per each time stamp,
    // the second element of tuple is a list that contains the entire data of all the rows belonging
    // to this time stamp.
    private transient MapState<Long, List<RowData>> inputState;

    private transient AggsHandleFunction function;

    // ------------------------------------------------------------------------
    // Metrics
    // ------------------------------------------------------------------------
    private static final String LATE_ELEMENTS_DROPPED_METRIC_NAME = "numLateRecordsDropped";
    private transient Counter numLateRecordsDropped;

    // ------------------------------------------------------------------------
    // Fields in original KeyedProcessFunctionWithCleanupState
    // ------------------------------------------------------------------------
    private final long minRetentionTime;
    private final long maxRetentionTime;
    protected final boolean stateCleaningEnabled;

    // holds the latest registered cleanup timer
    private ValueState<Long> cleanupTimeState;

    public AsyncRowTimeRowsBoundedPrecedingFunction(
            long minRetentionTime,
            long maxRetentionTime,
            GeneratedAggsHandleFunction genAggsHandler,
            LogicalType[] accTypes,
            LogicalType[] inputFieldTypes,
            long precedingOffset,
            int rowTimeIdx) {
        this.minRetentionTime = minRetentionTime;
        this.maxRetentionTime = maxRetentionTime;
        this.stateCleaningEnabled = minRetentionTime > 1;
        Preconditions.checkNotNull(precedingOffset);
        this.genAggsHandler = genAggsHandler;
        this.accTypes = accTypes;
        this.inputFieldTypes = inputFieldTypes;
        this.precedingOffset = precedingOffset;
        this.rowTimeIdx = rowTimeIdx;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        function = genAggsHandler.newInstance(getRuntimeContext().getUserCodeClassLoader());
        function.open(new PerKeyStateDataViewStore(getRuntimeContext()));

        output = new JoinedRowData();

        ValueStateDescriptor<Long> lastTriggeringTsDescriptor =
                new ValueStateDescriptor<>("lastTriggeringTsState", Types.LONG);
        lastTriggeringTsState =
                getStreamingRuntimeContext().getValueState(lastTriggeringTsDescriptor);

        ValueStateDescriptor<Long> dataCountStateDescriptor =
                new ValueStateDescriptor<Long>("processedCountState", Types.LONG);
        counterState = getStreamingRuntimeContext().getValueState(dataCountStateDescriptor);

        InternalTypeInfo<RowData> accTypeInfo = InternalTypeInfo.ofFields(accTypes);
        ValueStateDescriptor<RowData> accStateDesc =
                new ValueStateDescriptor<RowData>("accState", accTypeInfo);
        accState = getStreamingRuntimeContext().getValueState(accStateDesc);

        // input element are all binary row as they are came from network
        InternalTypeInfo<RowData> inputType = InternalTypeInfo.ofFields(inputFieldTypes);
        ListTypeInfo<RowData> rowListTypeInfo = new ListTypeInfo<RowData>(inputType);
        MapStateDescriptor<Long, List<RowData>> inputStateDesc =
                new MapStateDescriptor<Long, List<RowData>>(
                        "inputState", Types.LONG, rowListTypeInfo);
        inputState = getStreamingRuntimeContext().getMapState(inputStateDesc);

        initCleanupTimeState("RowTimeBoundedRowsOverCleanupTime");

        // metrics
        this.numLateRecordsDropped =
                getRuntimeContext().getMetricGroup().counter(LATE_ELEMENTS_DROPPED_METRIC_NAME);
    }

    @Override
    protected ThrowingConsumer<
                    Tuple3<
                            RowData,
                            KeyedProcessFunction<K, RowData, RowData>.Context,
                            Collector<RowData>>,
                    Exception>
            declareProcess(DeclarationContext context) throws DeclarationException {

        // ------------------------------------------------------------------------
        // variables for async state
        // ------------------------------------------------------------------------
        DeclaredVariable<RowData> inputVar =
                context.declareVariable(
                        InternalTypeInfo.ofFields(inputFieldTypes), "input", () -> null);

        /**
         * Corresponding to <a
         * href="https://alidocs.dingtalk.com/i/nodes/XPwkYGxZV347LdvpHYbo0pvBJAgozOKL">Q4</a>
         */
        // Context的TypeInformation和序列化器不太好写
        // DeclaredVariable<KeyedProcessFunction<K, RowData, RowData>.Context> ctx =
        //         context.declareVariable(
        //                 InternalTypeInfo.of(RowType.of(true, inputFieldTypes)),
        //                 "context",
        //                 () -> null);
        AtomicReference<KeyedProcessFunction<K, RowData, RowData>.Context> ctx =
                new AtomicReference<>();

        DeclaredVariable<Long> minRetentionTimeVar =
                context.declareVariable(
                        Types.LONG, "minRetentionTime", () -> this.minRetentionTime);

        DeclaredVariable<Long> maxRetentionTimeVar =
                context.declareVariable(
                        Types.LONG, "maxRetentionTime", () -> this.maxRetentionTime);

        DeclaredVariable<Long> triggeringTsVar =
                context.declareVariable(Types.LONG, "triggeringTs", () -> 0L);

        return context.<Tuple3<
                                RowData,
                                KeyedProcessFunction<K, RowData, RowData>.Context,
                                Collector<RowData>>,
                        Void>
                        declareChain(
                                e -> {
                                    inputVar.set(e.f0);
                                    ctx.set(e.f1);
                                    triggeringTsVar.set(e.f0.getLong(rowTimeIdx));
                                    return StateFutureUtils.completedVoidFuture();
                                })
                .thenCompose(empty -> cleanupTimeState.asyncValue())
                .thenCompose(
                        curCleanupTime -> {
                            if (!stateCleaningEnabled) {
                                return StateFutureUtils.completedVoidFuture();
                            }

                            long currentTime = ctx.get().timerService().currentProcessingTime();
                            return registerProcessingCleanupTimer(
                                    curCleanupTime,
                                    currentTime,
                                    minRetentionTimeVar.get(),
                                    maxRetentionTimeVar.get(),
                                    ctx.get().timerService());
                        })
                .thenCompose(empty -> lastTriggeringTsState.asyncValue())
                .thenApply(
                        lastTriggeringTs -> {
                            if (lastTriggeringTs == null) {
                                return 0L;
                            } else {
                                return lastTriggeringTs;
                            }
                        })
                .thenConditionallyAccept(
                        lastTriggeringTs -> triggeringTsVar.get() > lastTriggeringTs,
                        lastTriggeringTs ->
                                context.<Tuple3<
                                                        RowData,
                                                        KeyedProcessFunction<K, RowData, RowData>
                                                                .Context,
                                                        Collector<RowData>>,
                                                List<RowData>>
                                                declareChain(
                                                        e ->
                                                                inputState.asyncGet(
                                                                        triggeringTsVar.get()))
                                        .thenConditionallyCompose(
                                                Objects::nonNull,
                                                data -> {
                                                    data.add(inputVar.get());
                                                    return inputState.asyncPut(
                                                            triggeringTsVar.get(), data);
                                                },
                                                data -> {
                                                    data = new ArrayList<>();
                                                    data.add(inputVar.get());
                                                    return inputState.asyncPut(
                                                            triggeringTsVar.get(), data);
                                                })
                                        .thenAccept(
                                                tuple2 -> {
                                                    if (tuple2.f0) {
                                                        ctx.get()
                                                                .timerService()
                                                                .registerEventTimeTimer(
                                                                        triggeringTsVar.get());
                                                    }
                                                }),
                        lastTriggeringTs -> numLateRecordsDropped.inc())
                .finish();
    }

    //    @Override
    protected TriConsumerWithException<
                    RowData,
                    KeyedProcessFunction<K, RowData, RowData>.Context,
                    Collector<RowData>,
                    Exception>
            declareProcess2(DeclarationContext context) throws DeclarationException {
        // ------------------------------------------------------------------------
        // variables for async state
        // ------------------------------------------------------------------------
        DeclaredVariable<RowData> inputVar =
                context.declareVariable(
                        InternalTypeInfo.ofFields(inputFieldTypes), "input", () -> null);

        /**
         * Corresponding to <a
         * href="https://alidocs.dingtalk.com/i/nodes/XPwkYGxZV347LdvpHYbo0pvBJAgozOKL">Q4</a>
         */
        // Context的TypeInformation和序列化器不太好写
        // DeclaredVariable<KeyedProcessFunction<K, RowData, RowData>.Context> ctx =
        //         context.declareVariable(
        //                 InternalTypeInfo.of(RowType.of(true, inputFieldTypes)),
        //                 "context",
        //                 () -> null);

        DeclaredVariable<Boolean> stateCleaningEnabledVar =
                context.declareVariable(
                        Types.BOOLEAN, "stateCleaningEnabled", () -> stateCleaningEnabled);

        DeclaredVariable<Long> minRetentionTimeVar =
                context.declareVariable(
                        Types.LONG, "minRetentionTime", () -> this.minRetentionTime);

        DeclaredVariable<Long> maxRetentionTimeVar =
                context.declareVariable(
                        Types.LONG, "maxRetentionTime", () -> this.maxRetentionTime);

        DeclaredVariable<Long> lastTriggeringTsVar =
                context.declareVariable(Types.LONG, "lastTriggeringTs", () -> 0L);

        // ------------------------------------------------------------------------
        // callbacks for async state
        // ------------------------------------------------------------------------

        NamedFunction<RowData, StateFuture<Void>> prepareVarsCallBack =
                context.declare(
                        "prepareVars",
                        in -> {
                            inputVar.set(in);
                            return StateFutureUtils.completedVoidFuture();
                        });

        NamedFunction<Void, StateFuture<Long>> getCleanupTimeCallBack =
                context.declare(
                        "getCleanupTime",
                        empty -> {
                            return cleanupTimeState.asyncValue();
                        });

        /**
         * Corresponding to <a
         * href="https://alidocs.dingtalk.com/i/nodes/XPwkYGxZV347LdvpHYbo0pvBJAgozOKL">Q2</a>
         */
        // current cleanup time, current process time, context
        NamedFunction<
                        Tuple3<Long, Long, KeyedProcessFunction<K, RowData, RowData>.Context>,
                        StateFuture<Void>>
                registerProcessingCleanupTimerCallBack =
                        context.declare(
                                "registerProcessingCleanupTime",
                                (tuple) -> {
                                    if (!stateCleaningEnabledVar.get()) {
                                        return StateFutureUtils.completedVoidFuture();
                                    }
                                    Long curCleanupTime = tuple.f0;
                                    Long currentTime = tuple.f1;
                                    KeyedProcessFunction<K, RowData, RowData>.Context ctx =
                                            tuple.f2;

                                    return registerProcessingCleanupTimer(
                                            curCleanupTime,
                                            currentTime,
                                            minRetentionTimeVar.get(),
                                            maxRetentionTimeVar.get(),
                                            ctx.timerService());
                                });

        NamedFunction<Void, StateFuture<Void>> getLastTriggeringTsCallBack =
                context.declare(
                        "getLastTriggeringTs",
                        empty -> {
                            return lastTriggeringTsState
                                    .asyncValue()
                                    .thenAccept(
                                            lastTriggeringTs -> {
                                                if (lastTriggeringTs == null) {
                                                    lastTriggeringTsVar.set(0L);
                                                } else {
                                                    lastTriggeringTsVar.set(lastTriggeringTs);
                                                }
                                            });
                        });

        NamedFunction<Void, Boolean> checkDataExpiredCallBack =
                context.declare(
                        "checkDataExpired",
                        empty -> inputVar.get().getLong(rowTimeIdx) > lastTriggeringTsVar.get());

        NamedFunction<Void, StateFuture<List<RowData>>> getOldDataCallBack =
                context.declare(
                        "getOldData",
                        empty -> {
                            long triggeringTs = inputVar.get().getLong(rowTimeIdx);
                            return inputState.asyncGet(triggeringTs);
                        });

        NamedBiFunction<
                        List<RowData>,
                        KeyedProcessFunction<K, RowData, RowData>.Context,
                        StateFuture<Void>>
                saveDataCallBackWithOriginalDataExists =
                        context.declare(
                                "saveDataWithOriginalDataExists",
                                (dataInState, ctx) -> {
                                    long triggeringTs = inputVar.get().getLong(rowTimeIdx);
                                    if (dataInState == null) {
                                        dataInState = new ArrayList<>();
                                    }
                                    dataInState.add(inputVar.get());

                                    return inputState
                                            .asyncPut(triggeringTs, dataInState)
                                            .thenAccept(
                                                    empty -> {
                                                        ctx.timerService()
                                                                .registerEventTimeTimer(
                                                                        triggeringTs);
                                                    });
                                });

        // input, lastTriggeringTs, context
        NamedFunction<KeyedProcessFunction<K, RowData, RowData>.Context, StateFuture<Void>>
                saveDataCallBack =
                        context.declare(
                                "saveData",
                                ctx -> {
                                    RowData input = inputVar.get();
                                    // triggering timestamp for trigger calculation
                                    long triggeringTs = input.getLong(rowTimeIdx);
                                    return inputState
                                            .asyncGet(triggeringTs)
                                            .thenCompose(
                                                    data -> {
                                                        if (data != null) {
                                                            data.add(inputVar.get());
                                                            return inputState.asyncPut(
                                                                    triggeringTs, data);
                                                        } else {
                                                            data = new ArrayList<>();
                                                            data.add(input);
                                                            return inputState
                                                                    .asyncPut(triggeringTs, data)
                                                                    .thenAccept(
                                                                            empty ->
                                                                                    ctx.timerService()
                                                                                            .registerEventTimeTimer(
                                                                                                    triggeringTs));
                                                        }
                                                    });
                                });

        NamedFunction<Void, Void> numLateRecordsDroppedCallBack =
                context.declare(
                        "numLateRecordsDropped",
                        empty -> {
                            numLateRecordsDropped.inc();
                            return null;
                        });

        return (in, ctx, collector) ->
                prepareVarsCallBack
                        .apply(in)
                        .thenCompose(getCleanupTimeCallBack)
                        .thenCompose(
                                curCleanupTime ->
                                        registerProcessingCleanupTimerCallBack.apply(
                                                Tuple3.of(
                                                        curCleanupTime,
                                                        ctx.timerService().currentProcessingTime(),
                                                        ctx)))
                        .thenCompose(getLastTriggeringTsCallBack)
                        .thenConditionallyApply(
                                checkDataExpiredCallBack,
                                empty -> saveDataCallBack.apply(ctx),
                                numLateRecordsDroppedCallBack);
    }

    /**
     * Puts an element from the input stream into state if it is not late. Registers a timer for the
     * next watermark.
     *
     * @param input The input value.
     * @param ctx A {@link Context} that allows querying the timestamp of the element and getting
     *     TimerService for registering timers and querying the time. The context is only valid
     *     during the invocation of this method, do not store it.
     * @param out The collector for returning result values.
     * @throws Exception
     */
    @Override
    public void processElement(
            RowData input,
            KeyedProcessFunction<K, RowData, RowData>.Context ctx,
            Collector<RowData> out)
            throws Exception {
        // move the logic to #declareProcess
        throw new IllegalStateException("should not be called");
    }

    @Override
    public void onTimer(
            long timestamp,
            KeyedProcessFunction<K, RowData, RowData>.OnTimerContext ctx,
            Collector<RowData> out)
            throws Exception {
        if (isProcessingTimeTimer(ctx)) {
            onProcessingTimer(timestamp, ctx, out);
        } else {
            onEventTimer(timestamp, ctx, out);
        }
    }

    private void onProcessingTimer(
            long timestamp,
            KeyedProcessFunction<K, RowData, RowData>.OnTimerContext ctx,
            Collector<RowData> out) {
        if (!stateCleaningEnabled) {
            return;
        }
        StateFuture<Long> lastProcessedTimeFuture =
                lastTriggeringTsState
                        .asyncValue()
                        .thenApply(
                                lastProcessedTime -> {
                                    if (lastProcessedTime == null) {
                                        lastProcessedTime = 0L;
                                    }
                                    return lastProcessedTime;
                                });
        inputState
                .asyncKeys()
                .thenCombine(
                        lastProcessedTimeFuture,
                        (keysIt, lastProcessedTime) -> {
                            // is data left which has not been processed yet?
                            AtomicBoolean noRecordsToProcess = new AtomicBoolean(true);
                            /**
                             * Corresponding to <a
                             * href="https://alidocs.dingtalk.com/i/nodes/XPwkYGxZV347LdvpHYbo0pvBJAgozOKL">Q6</a>
                             */
                            keysIt.onNext(
                                    key -> {
                                        if (key > lastProcessedTime) {
                                            noRecordsToProcess.set(false);
                                        }
                                        return null;
                                    });
                            return noRecordsToProcess.get();
                        })
                .thenAccept(
                        noRecordsToProcess -> {
                            if (noRecordsToProcess) {
                                // We clean the state
                                cleanupState(
                                                inputState,
                                                accState,
                                                counterState,
                                                lastTriggeringTsState)
                                        .thenAccept(empty -> function.cleanup());
                                return;
                            }

                            // There are records left to process because a watermark has not been
                            // received yet.
                            // This would only happen if the input stream has stopped. So we don't
                            // need to clean up.
                            // We leave the state as it is and schedule a new cleanup timer
                            cleanupTimeState
                                    .asyncValue()
                                    .thenAccept(
                                            curCleanupTime ->
                                                    registerProcessingCleanupTimer(
                                                            curCleanupTime,
                                                            ctx.timerService()
                                                                    .currentProcessingTime(),
                                                            minRetentionTime,
                                                            maxRetentionTime,
                                                            ctx.timerService()));
                        });
    }

    private void onEventTimer(
            long timestamp,
            KeyedProcessFunction<K, RowData, RowData>.OnTimerContext ctx,
            Collector<RowData> out) {
        // gets all window data from state for the calculation
        inputState
                .asyncGet(timestamp)
                .thenCompose(
                        inputs -> {
                            if (inputs == null) {
                                return lastTriggeringTsState
                                        .asyncUpdate(timestamp)
                                        .thenCompose(empty -> cleanupTimeState.asyncValue())
                                        .thenAccept(
                                                curCleanupTime ->
                                                        registerProcessingCleanupTimer(
                                                                curCleanupTime,
                                                                ctx.timerService()
                                                                        .currentProcessingTime(),
                                                                minRetentionTime,
                                                                maxRetentionTime,
                                                                ctx.timerService()));
                            }

                            StateFuture<Long> dataCountFuture = counterState.asyncValue();
                            return accState.asyncValue()
                                    .thenCombine(
                                            dataCountFuture,
                                            (accumulators, dataCount) -> {
                                                if (dataCount == null) {
                                                    dataCount = 0L;
                                                }
                                                if (accumulators == null) {
                                                    accumulators = function.createAccumulators();
                                                }

                                                // set accumulators in context first
                                                function.setAccumulators(accumulators);

                                                final AtomicReference<List<RowData>> retractList =
                                                        new AtomicReference<>(null);
                                                final AtomicLong retractTs =
                                                        new AtomicLong(Long.MAX_VALUE);
                                                final AtomicInteger retractCnt =
                                                        new AtomicInteger(0);

                                                StateFuture<Void> loopMergedFuture =
                                                        StateFutureUtils.completedVoidFuture();

                                                AtomicInteger i = new AtomicInteger(0);
                                                while (i.get() < inputs.size()) {
                                                    RowData input = inputs.get(i.get());
                                                    final AtomicReference<RowData> retractRow =
                                                            new AtomicReference<>(null);

                                                    StateFuture<Void> findRetractRowCallBack;
                                                    if (dataCount >= precedingOffset) {
                                                        StateFuture<Void> initRetractRowsFuture;
                                                        if (null == retractList.get()) {
                                                            // find the smallest timestamp
                                                            initRetractRowsFuture =
                                                                    loopMergedFuture
                                                                            .thenCompose(
                                                                                    empty ->
                                                                                            getOldestRows())
                                                                            .thenAccept(
                                                                                    oldestRows -> {
                                                                                        retractTs
                                                                                                .set(
                                                                                                        oldestRows
                                                                                                                .getKey());
                                                                                        retractList
                                                                                                .set(
                                                                                                        oldestRows
                                                                                                                .getValue());
                                                                                    });
                                                        } else {
                                                            initRetractRowsFuture =
                                                                    StateFutureUtils
                                                                            .completedVoidFuture();
                                                        }

                                                        findRetractRowCallBack =
                                                                initRetractRowsFuture.thenCompose(
                                                                        empty -> {
                                                                            if (retractList.get()
                                                                                    != null) {
                                                                                retractRow.set(
                                                                                        retractList
                                                                                                .get()
                                                                                                .get(
                                                                                                        retractCnt
                                                                                                                .get()));
                                                                                retractCnt
                                                                                        .addAndGet(
                                                                                                1);

                                                                                // remove
                                                                                // retracted
                                                                                // values
                                                                                // from
                                                                                // state
                                                                                if (retractList
                                                                                                .get()
                                                                                                .size()
                                                                                        == retractCnt
                                                                                                .get()) {
                                                                                    return inputState
                                                                                            .asyncRemove(
                                                                                                    retractTs
                                                                                                            .get())
                                                                                            .thenAccept(
                                                                                                    empty2 -> {
                                                                                                        retractList
                                                                                                                .set(
                                                                                                                        null);
                                                                                                        retractCnt
                                                                                                                .set(
                                                                                                                        0);
                                                                                                    });
                                                                                }
                                                                            }
                                                                            return StateFutureUtils
                                                                                    .completedVoidFuture();
                                                                        });
                                                    } else {
                                                        dataCount += 1;
                                                        findRetractRowCallBack = loopMergedFuture;
                                                    }

                                                    loopMergedFuture =
                                                            findRetractRowCallBack.thenAccept(
                                                                    empty -> {
                                                                        // retract old row from
                                                                        // accumulators
                                                                        if (null
                                                                                != retractRow
                                                                                        .get()) {
                                                                            function.retract(
                                                                                    retractRow
                                                                                            .get());
                                                                        }

                                                                        // accumulate current row
                                                                        function.accumulate(input);

                                                                        // prepare output row
                                                                        output.replace(
                                                                                input,
                                                                                function
                                                                                        .getValue());
                                                                        out.collect(output);

                                                                        i.addAndGet(1);
                                                                    });
                                                }

                                                Long finalDataCount = dataCount;
                                                return loopMergedFuture
                                                        .thenCompose(
                                                                empty ->
                                                                        inputState.asyncContains(
                                                                                retractTs.get()))
                                                        .thenCompose(
                                                                containsTs -> {
                                                                    // update all states
                                                                    if (containsTs
                                                                            && retractCnt.get()
                                                                                    > 0) {
                                                                        List<RowData> list =
                                                                                retractList.get();
                                                                        list.subList(
                                                                                        0,
                                                                                        retractCnt
                                                                                                .get())
                                                                                .clear();
                                                                        return inputState.asyncPut(
                                                                                retractTs.get(),
                                                                                list);
                                                                    }
                                                                    return StateFutureUtils
                                                                            .completedVoidFuture();
                                                                })
                                                        .thenAccept(
                                                                empty ->
                                                                        counterState.asyncUpdate(
                                                                                finalDataCount))
                                                        .thenAccept(
                                                                empty -> {
                                                                    // update the value of
                                                                    // accumulators for future
                                                                    // incremental computation
                                                                    RowData finalAcc =
                                                                            function
                                                                                    .getAccumulators();
                                                                    accState.asyncUpdate(finalAcc);
                                                                });
                                            })
                                    .thenCompose(empty -> empty);
                        })
                .thenAccept(empty -> lastTriggeringTsState.asyncUpdate(timestamp))
                .thenCompose(empty -> cleanupTimeState.asyncValue())
                .thenAccept(
                        curCleanupTime ->
                                // update cleanup timer
                                registerProcessingCleanupTimer(
                                        curCleanupTime,
                                        ctx.timerService().currentProcessingTime(),
                                        minRetentionTime,
                                        maxRetentionTime,
                                        ctx.timerService()));
    }

    @Override
    public void close() throws Exception {
        if (null != function) {
            function.close();
        }
    }

    private void initCleanupTimeState(String stateName) {
        if (stateCleaningEnabled) {
            ValueStateDescriptor<Long> inputCntDescriptor =
                    new ValueStateDescriptor<>(stateName, Types.LONG);
            cleanupTimeState = getStreamingRuntimeContext().getValueState(inputCntDescriptor);
        }
    }

    private StreamingRuntimeContext getStreamingRuntimeContext() {
        return (StreamingRuntimeContext) getRuntimeContext();
    }

    private StateFuture<Void> registerProcessingCleanupTimer(
            @Nullable Long curCleanupTime,
            long currentTime,
            long minRetentionTime,
            long maxRetentionTime,
            TimerService timerService) {
        // check if a cleanup timer is registered and
        // that the current cleanup timer won't delete state we need to keep
        if (curCleanupTime == null || (currentTime + minRetentionTime) > curCleanupTime) {
            // we need to register a new (later) timer
            long cleanupTime = currentTime + maxRetentionTime;
            // register timer and remember clean-up time
            timerService.registerProcessingTimeTimer(cleanupTime);
            // delete expired timer
            if (curCleanupTime != null) {
                timerService.deleteProcessingTimeTimer(curCleanupTime);
            }
            return cleanupTimeState.asyncUpdate(cleanupTime);
        }
        return StateFutureUtils.completedVoidFuture();
    }

    private boolean isProcessingTimeTimer(OnTimerContext ctx) {
        return ctx.timeDomain() == TimeDomain.PROCESSING_TIME;
    }

    private StateFuture<Collection<Void>> cleanupState(State... states) {
        List<StateFuture<Void>> futures = new ArrayList<>();
        for (final State state : states) {
            futures.add(state.asyncClear());
        }
        futures.add(this.cleanupTimeState.asyncClear());

        return StateFutureUtils.combineAll(futures);
    }

    private StateFuture<Map.Entry<Long, List<RowData>>> getOldestRows() {
        AtomicLong minTs = new AtomicLong(Long.MAX_VALUE);
        AtomicReference<Map.Entry<Long, List<RowData>>> minEntry = new AtomicReference<>(null);
        return inputState
                .asyncEntries()
                .thenAccept(
                        it ->
                                it.onNext(
                                        entry -> {
                                            Long dataTs = entry.getKey();
                                            if (dataTs < minTs.get()) {
                                                minTs.set(dataTs);
                                                minEntry.set(entry);
                                            }
                                        }))
                .thenApply(empty -> minEntry.get());
    }
}
