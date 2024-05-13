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

package org.apache.flink.table.runtime.operators.join.stream.asyn.state;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.v2.MapState;
import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.runtime.state.v2.MapStateDescriptor;
import org.apache.flink.runtime.state.v2.ValueStateDescriptor;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.operators.join.stream.state.OuterJoinRecordStateView;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.table.runtime.util.StateConfigUtil.createTtlConfig;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Utility to create a {@link AsyncJoinRecordStateViews} depends on {@link JoinInputSideSpec}. */
public final class AsyncJoinRecordStateViews {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncJoinRecordStateViews.class);

    /** Creates a {@link OuterJoinRecordStateView} depends on {@link JoinInputSideSpec}. */
    public static AsyncJoinRecordStateView create(
            StreamingRuntimeContext ctx,
            String stateName,
            JoinInputSideSpec inputSideSpec,
            InternalTypeInfo<RowData> recordType,
            long retentionTime) {
        LOG.info(
                "Creating AsyncOuterJoinRecordStateView for stateName: {}, {}, {}",
                stateName,
                inputSideSpec,
                recordType);
        StateTtlConfig ttlConfig = createTtlConfig(retentionTime);
        if (inputSideSpec.hasUniqueKey()) {
            if (inputSideSpec.joinKeyContainsUniqueKey()) {
                return new AsyncJoinKeyContainsUniqueKey(ctx, stateName, recordType, ttlConfig);
            } else {
                return new AsyncInputSideHasUniqueKey(
                        ctx,
                        stateName,
                        recordType,
                        inputSideSpec.getUniqueKeyType(),
                        inputSideSpec.getUniqueKeySelector(),
                        ttlConfig);
            }
        } else {
            return new AsyncInputSideHasNoUniqueKey(ctx, stateName, recordType, ttlConfig);
        }
    }

    // ------------------------------------------------------------------------------------------

    private static final class AsyncJoinKeyContainsUniqueKey implements AsyncJoinRecordStateView {

        private final ValueState<Tuple2<RowData, Integer>> recordState;

        private AsyncJoinKeyContainsUniqueKey(
                StreamingRuntimeContext ctx,
                String stateName,
                InternalTypeInfo<RowData> recordType,
                StateTtlConfig ttlConfig) {
            TupleTypeInfo<Tuple2<RowData, Integer>> valueTypeInfo =
                    new TupleTypeInfo<>(recordType, Types.INT);
            ValueStateDescriptor<Tuple2<RowData, Integer>> recordStateDesc =
                    new ValueStateDescriptor<>(stateName, valueTypeInfo);
            if (ttlConfig.isEnabled()) {
                recordStateDesc.enableTimeToLive(ttlConfig);
            }
            this.recordState = ctx.getValueState(recordStateDesc);
        }

        @Override
        public StateFuture<Void> addRecord(RowData record) throws Exception {
            return addRecord(record, -1);
        }

        @Override
        public StateFuture<Void> addRecord(RowData record, int numOfAssociations) throws Exception {
            return recordState.asyncUpdate(Tuple2.of(record, numOfAssociations));
        }

        @Override
        public StateFuture<Void> updateNumOfAssociations(RowData record, int numOfAssociations)
                throws Exception {
            return recordState.asyncUpdate(Tuple2.of(record, numOfAssociations));
        }

        @Override
        public StateFuture<Void> retractRecord(RowData record) throws Exception {
            return recordState.asyncClear();
        }

        @Override
        public State getRawState() {
            return recordState;
        }
    }

    private static final class AsyncInputSideHasUniqueKey implements AsyncJoinRecordStateView {

        // stores record in the mapping <UK, <Record, associated-num>>
        private final MapState<RowData, Tuple2<RowData, Integer>> recordState;
        private final KeySelector<RowData, RowData> uniqueKeySelector;

        private AsyncInputSideHasUniqueKey(
                StreamingRuntimeContext ctx,
                String stateName,
                InternalTypeInfo<RowData> recordType,
                InternalTypeInfo<RowData> uniqueKeyType,
                KeySelector<RowData, RowData> uniqueKeySelector,
                StateTtlConfig ttlConfig) {
            checkNotNull(uniqueKeyType);
            checkNotNull(uniqueKeySelector);
            TupleTypeInfo<Tuple2<RowData, Integer>> valueTypeInfo =
                    new TupleTypeInfo<>(recordType, Types.INT);
            MapStateDescriptor<RowData, Tuple2<RowData, Integer>> recordStateDesc =
                    new MapStateDescriptor<>(stateName, uniqueKeyType, valueTypeInfo);
            if (ttlConfig.isEnabled()) {
                recordStateDesc.enableTimeToLive(ttlConfig);
            }
            this.recordState = ctx.getMapState(recordStateDesc);
            this.uniqueKeySelector = uniqueKeySelector;
        }

        @Override
        public StateFuture<Void> addRecord(RowData record) throws Exception {
            return addRecord(record, -1);
        }

        @Override
        public StateFuture<Void> addRecord(RowData record, int numOfAssociations) throws Exception {
            RowData uniqueKey = uniqueKeySelector.getKey(record);
            return recordState.asyncPut(uniqueKey, Tuple2.of(record, numOfAssociations));
        }

        @Override
        public StateFuture<Void> updateNumOfAssociations(RowData record, int numOfAssociations)
                throws Exception {
            RowData uniqueKey = uniqueKeySelector.getKey(record);
            return recordState.asyncPut(uniqueKey, Tuple2.of(record, numOfAssociations));
        }

        @Override
        public StateFuture<Void> retractRecord(RowData record) throws Exception {
            RowData uniqueKey = uniqueKeySelector.getKey(record);
            return recordState.asyncRemove(uniqueKey);
        }

        @Override
        public State getRawState() {
            return recordState;
        }
    }

    private static final class AsyncInputSideHasNoUniqueKey implements AsyncJoinRecordStateView {

        // stores record in the mapping <Record, <appear-times, associated-num>>
        private final MapState<RowData, Tuple2<Integer, Integer>> recordState;

        private AsyncInputSideHasNoUniqueKey(
                StreamingRuntimeContext ctx,
                String stateName,
                InternalTypeInfo<RowData> recordType,
                StateTtlConfig ttlConfig) {
            TupleTypeInfo<Tuple2<Integer, Integer>> tupleTypeInfo =
                    new TupleTypeInfo<>(Types.INT, Types.INT);
            MapStateDescriptor<RowData, Tuple2<Integer, Integer>> recordStateDesc =
                    new MapStateDescriptor<>(stateName, recordType, tupleTypeInfo);
            if (ttlConfig.isEnabled()) {
                recordStateDesc.enableTimeToLive(ttlConfig);
            }
            this.recordState = ctx.getMapState(recordStateDesc);
        }

        @Override
        public StateFuture<Void> addRecord(RowData record) throws Exception {
            return addRecord(record, -1);
        }

        @Override
        public StateFuture<Void> addRecord(RowData record, int numOfAssociations) throws Exception {
            return recordState
                    .asyncGet(record)
                    .thenAccept(
                            tuple -> {
                                if (tuple != null) {
                                    tuple.f0 = tuple.f0 + 1;
                                    tuple.f1 = numOfAssociations;
                                } else {
                                    tuple = Tuple2.of(1, numOfAssociations);
                                }
                                recordState.asyncPut(record, tuple);
                            });
        }

        @Override
        public StateFuture<Void> updateNumOfAssociations(RowData record, int numOfAssociations)
                throws Exception {
            return recordState
                    .asyncGet(record)
                    .thenAccept(
                            tuple -> {
                                if (tuple != null) {
                                    tuple.f1 = numOfAssociations;
                                } else {
                                    // compatible for state ttl
                                    tuple = Tuple2.of(1, numOfAssociations);
                                }
                                recordState.asyncPut(record, tuple);
                            });
        }

        @Override
        public StateFuture<Void> retractRecord(RowData record) throws Exception {
            return recordState
                    .asyncGet(record)
                    .thenAccept(
                            tuple -> {
                                if (tuple != null) {
                                    if (tuple.f0 > 1) {
                                        tuple.f0 = tuple.f0 - 1;
                                        recordState.asyncPut(record, tuple);
                                    } else {
                                        recordState.asyncRemove(record);
                                    }
                                }
                            });
        }

        @Override
        public State getRawState() {
            return recordState;
        }
    }
}
