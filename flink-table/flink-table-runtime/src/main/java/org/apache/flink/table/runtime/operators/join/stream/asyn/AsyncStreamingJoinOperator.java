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

package org.apache.flink.table.runtime.operators.join.stream.asyn;

import org.apache.flink.api.common.state.v2.MapState;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.stream.asyn.state.AsyncJoinRecordStateView;
import org.apache.flink.table.runtime.operators.join.stream.asyn.state.AsyncJoinRecordStateViews;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;

import java.util.concurrent.atomic.AtomicInteger;

/** Asynchronous streaming unbounded Join operator which supports INNER/LEFT/RIGHT/FULL JOIN. */
public class AsyncStreamingJoinOperator extends AsyncAbstractStreamingJoinOperator {

    private static final long serialVersionUID = 1L;

    // whether left side is outer side, e.g. left is outer but right is not when LEFT OUTER JOIN
    protected final boolean leftIsOuter;
    // whether right side is outer side, e.g. right is outer but left is not when RIGHT OUTER JOIN
    protected final boolean rightIsOuter;

    private transient JoinedRowData outRow;
    private transient RowData leftNullRow;
    private transient RowData rightNullRow;

    // left join state
    protected transient AsyncJoinRecordStateView leftRecordStateView;
    // right join state
    protected transient AsyncJoinRecordStateView rightRecordStateView;

    protected transient boolean leftContainUniqueKeys;
    protected transient boolean rightContainUniqueKeys;

    public AsyncStreamingJoinOperator(
            InternalTypeInfo<RowData> leftType,
            InternalTypeInfo<RowData> rightType,
            GeneratedJoinCondition generatedJoinCondition,
            JoinInputSideSpec leftInputSideSpec,
            JoinInputSideSpec rightInputSideSpec,
            boolean leftIsOuter,
            boolean rightIsOuter,
            boolean[] filterNullKeys,
            long leftStateRetentionTime,
            long rightStateRetentionTime) {
        super(
                leftType,
                rightType,
                generatedJoinCondition,
                leftInputSideSpec,
                rightInputSideSpec,
                filterNullKeys,
                leftStateRetentionTime,
                rightStateRetentionTime);
        this.leftIsOuter = leftIsOuter;
        this.rightIsOuter = rightIsOuter;
    }

    @Override
    public void open() throws Exception {
        super.open();

        this.outRow = new JoinedRowData();
        this.leftNullRow = new GenericRowData(leftType.toRowSize());
        this.rightNullRow = new GenericRowData(rightType.toRowSize());
        this.leftContainUniqueKeys =
                leftInputSideSpec.hasUniqueKey() && leftInputSideSpec.joinKeyContainsUniqueKey();
        this.rightContainUniqueKeys =
                rightInputSideSpec.hasUniqueKey() && rightInputSideSpec.joinKeyContainsUniqueKey();
        // initialize states
        this.leftRecordStateView =
                AsyncJoinRecordStateViews.create(
                        getRuntimeContext(),
                        "left-records",
                        leftInputSideSpec,
                        leftType,
                        leftStateRetentionTime);
        this.rightRecordStateView =
                AsyncJoinRecordStateViews.create(
                        getRuntimeContext(),
                        "right-records",
                        rightInputSideSpec,
                        rightType,
                        rightStateRetentionTime);
    }

    @Override
    public void processElement1(StreamRecord<RowData> element) throws Exception {
        processElement(
                element.getValue(),
                leftRecordStateView,
                rightRecordStateView,
                true,
                false,
                rightInputSideSpec.hasUniqueKey(),
                rightContainUniqueKeys);
    }

    @Override
    public void processElement2(StreamRecord<RowData> element) throws Exception {
        processElement(
                element.getValue(),
                rightRecordStateView,
                leftRecordStateView,
                false,
                false,
                leftInputSideSpec.hasUniqueKey(),
                leftContainUniqueKeys);
    }

    /**
     * Process an input element and output incremental joined records, retraction messages will be
     * sent in some scenarios.
     *
     * <p>Following is the pseudo code to describe the core logic of this method. The logic of this
     * method is too complex, so we provide the pseudo code to help understand the logic. We should
     * keep sync the following pseudo code with the real logic of the method.
     *
     * <p>Note: "+I" represents "INSERT", "-D" represents "DELETE", "+U" represents "UPDATE_AFTER",
     * "-U" represents "UPDATE_BEFORE". We forward input RowKind if it is inner join, otherwise, we
     * always send insert and delete for simplification. We can optimize this to send -U & +U
     * instead of D & I in the future (see FLINK-17337). They are equivalent in this join case. It
     * may need some refactoring if we want to send -U & +U, so we still keep -D & +I for now for
     * simplification. See {@code
     * FlinkChangelogModeInferenceProgram.SatisfyModifyKindSetTraitVisitor}.
     *
     * <pre>
     * if input record is accumulate
     * |  if input side is outer
     * |  |  if there is no matched rows on the other side, send +I[record+null], state.add(record, 0)
     * |  |  if there are matched rows on the other side
     * |  |  | if other side is outer
     * |  |  | |  if the matched num in the matched rows == 0, send -D[null+other]
     * |  |  | |  if the matched num in the matched rows > 0, skip
     * |  |  | |  otherState.update(other, old + 1)
     * |  |  | endif
     * |  |  | send +I[record+other]s, state.add(record, other.size)
     * |  |  endif
     * |  endif
     * |  if input side not outer
     * |  |  state.add(record)
     * |  |  if there is no matched rows on the other side, skip
     * |  |  if there are matched rows on the other side
     * |  |  |  if other side is outer
     * |  |  |  |  if the matched num in the matched rows == 0, send -D[null+other]
     * |  |  |  |  if the matched num in the matched rows > 0, skip
     * |  |  |  |  otherState.update(other, old + 1)
     * |  |  |  |  send +I[record+other]s
     * |  |  |  else
     * |  |  |  |  send +I/+U[record+other]s (using input RowKind)
     * |  |  |  endif
     * |  |  endif
     * |  endif
     * endif
     *
     * if input record is retract
     * |  state.retract(record)
     * |  if there is no matched rows on the other side
     * |  | if input side is outer, send -D[record+null]
     * |  endif
     * |  if there are matched rows on the other side, send -D[record+other]s if outer, send -D/-U[record+other]s if inner.
     * |  |  if other side is outer
     * |  |  |  if the matched num in the matched rows == 0, this should never happen!
     * |  |  |  if the matched num in the matched rows == 1, send +I[null+other]
     * |  |  |  if the matched num in the matched rows > 1, skip
     * |  |  |  otherState.update(other, old - 1)
     * |  |  endif
     * |  endif
     * endif
     * </pre>
     *
     * @param input the input element
     * @param inputSideStateView state of input side
     * @param otherSideStateView state of other side
     * @param inputIsLeft whether input side is left side
     * @param isSuppress whether suppress the output of redundant messages when the other side is
     *     outer join. This only applies to the case of mini-batch.
     */
    protected void processElement(
            RowData input,
            AsyncJoinRecordStateView inputSideStateView,
            AsyncJoinRecordStateView otherSideStateView,
            boolean inputIsLeft,
            boolean isSuppress,
            boolean hasUniqueKeys,
            boolean containUniqueKeys)
            throws Exception {
        boolean inputIsOuter = inputIsLeft ? leftIsOuter : rightIsOuter;
        boolean otherIsOuter = inputIsLeft ? rightIsOuter : leftIsOuter;
        boolean isAccumulateMsg = RowDataUtil.isAccumulateMsg(input);
        RowKind inputRowKind = input.getRowKind();
        input.setRowKind(RowKind.INSERT); // erase RowKind for later state updating
        if (containUniqueKeys) {
            ((ValueState<Tuple2<RowData, Integer>>) otherSideStateView.getRawState())
                    .asyncValue()
                    .thenAccept(
                            val -> {
                                boolean matched = (val != null);
                                OuterRecord outerRecord = null;
                                if (matched) {
                                    RowData other = val.f0;
                                    matched =
                                            inputIsLeft
                                                    ? joinCondition.apply(input, other)
                                                    : joinCondition.apply(other, input);
                                    int numOfAssociations = otherIsOuter ? val.f1 : -1;
                                    outerRecord = new OuterRecord(other, numOfAssociations);
                                }
                                tryMatchSingleRecord(
                                        input,
                                        otherSideStateView,
                                        isAccumulateMsg,
                                        inputIsOuter,
                                        otherIsOuter,
                                        matched,
                                        outerRecord,
                                        isSuppress,
                                        inputIsLeft,
                                        inputRowKind);

                                updateInputSideOnFinishIter(
                                        isAccumulateMsg,
                                        inputIsOuter,
                                        isSuppress,
                                        inputIsLeft,
                                        matched ? 1 : 0,
                                        input,
                                        inputSideStateView);
                            });
        } else if (hasUniqueKeys) {
            AtomicInteger matchedNum = new AtomicInteger(0);
            ((MapState<RowData, Tuple2<RowData, Integer>>) otherSideStateView.getRawState())
                    .asyncEntries()
                    .thenAccept(
                            iterator -> {
                                iterator.onNext(
                                                entry -> {
                                                    boolean matched = true;
                                                    if (entry == null
                                                            || entry.getKey() == null
                                                            || entry.getValue() == null) {
                                                        matched = false;
                                                    }

                                                    OuterRecord outerRecord = null;
                                                    if (matched) {
                                                        RowData other = entry.getValue().f0;
                                                        matched =
                                                                inputIsLeft
                                                                        ? joinCondition.apply(
                                                                                input, other)
                                                                        : joinCondition.apply(
                                                                                other, input);
                                                        int numOfAssociations =
                                                                otherIsOuter
                                                                        ? entry.getValue().f1
                                                                        : -1;
                                                        outerRecord =
                                                                new OuterRecord(
                                                                        other, numOfAssociations);
                                                    }

                                                    if (matched) {
                                                        matchedNum.incrementAndGet();
                                                    }
                                                    try {
                                                        tryMatchSingleRecord(
                                                                input,
                                                                otherSideStateView,
                                                                isAccumulateMsg,
                                                                inputIsOuter,
                                                                otherIsOuter,
                                                                matched,
                                                                outerRecord,
                                                                isSuppress,
                                                                inputIsLeft,
                                                                inputRowKind);
                                                    } catch (Exception e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                })
                                        .thenAccept(
                                                VOID -> { // update input side
                                                    updateInputSideOnFinishIter(
                                                            isAccumulateMsg,
                                                            inputIsOuter,
                                                            isSuppress,
                                                            inputIsLeft,
                                                            matchedNum.get(),
                                                            input,
                                                            inputSideStateView);
                                                });
                            });
        } else {
            AtomicInteger matchedNum = new AtomicInteger(0);
            ((MapState<RowData, Tuple2<Integer, Integer>>) otherSideStateView.getRawState())
                    .asyncEntries()
                    .thenAccept(
                            iterator -> {
                                iterator.onNext(
                                                entry -> {
                                                    boolean matched = true;
                                                    if (entry == null
                                                            || entry.getKey() == null
                                                            || entry.getValue() == null) {
                                                        matched = false;
                                                    }

                                                    OuterRecord outerRecord = null;
                                                    int freq = 0;
                                                    if (matched) {
                                                        RowData other = entry.getKey();
                                                        matched =
                                                                inputIsLeft
                                                                        ? joinCondition.apply(
                                                                                input, other)
                                                                        : joinCondition.apply(
                                                                                other, input);
                                                        freq = entry.getValue().f0;
                                                        int numOfAssociations =
                                                                otherIsOuter
                                                                        ? entry.getValue().f1
                                                                        : -1;
                                                        outerRecord =
                                                                new OuterRecord(
                                                                        other, numOfAssociations);
                                                    }

                                                    if (matched) {
                                                        matchedNum.addAndGet(freq);
                                                    }
                                                    try {
                                                        for (int i = 0;
                                                                i < freq;
                                                                i++) { // expand freq times
                                                            tryMatchSingleRecord(
                                                                    input,
                                                                    otherSideStateView,
                                                                    isAccumulateMsg,
                                                                    inputIsOuter,
                                                                    otherIsOuter,
                                                                    matched,
                                                                    outerRecord,
                                                                    isSuppress,
                                                                    inputIsLeft,
                                                                    inputRowKind);
                                                        }
                                                    } catch (Exception e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                })
                                        .thenAccept(
                                                VOID -> { // update input side
                                                    updateInputSideOnFinishIter(
                                                            isAccumulateMsg,
                                                            inputIsOuter,
                                                            isSuppress,
                                                            inputIsLeft,
                                                            matchedNum.get(),
                                                            input,
                                                            inputSideStateView);
                                                });
                            });
        }
    }

    // -------------------------------------------------------------------------------------

    private void tryMatchSingleRecord(
            RowData input,
            AsyncJoinRecordStateView otherSideStateView,
            boolean isAccumulateMsg,
            boolean inputIsOuter,
            boolean otherIsOuter,
            boolean matched,
            OuterRecord outerRecord,
            boolean isSuppress,
            boolean inputIsLeft,
            RowKind inputRowKind)
            throws Exception {
        if (isAccumulateMsg) {
            if (inputIsOuter) {
                if (matched) { // there are matched rows on the other side
                    if (otherIsOuter) {
                        // if the matched num in the matched rows == 0
                        if (outerRecord.numOfAssociations == 0 && !isSuppress) {
                            // send -D[null+other]
                            outRow.setRowKind(RowKind.DELETE);
                            outputNullPadding(outerRecord.record, !inputIsLeft);
                        } // ignore matched number > 0
                        // otherState.update(other, old + 1)
                        otherSideStateView.updateNumOfAssociations(
                                outerRecord.record, outerRecord.numOfAssociations + 1);
                    }
                    // send +I[record+other]s
                    outRow.setRowKind(RowKind.INSERT);
                    output(input, outerRecord.record, inputIsLeft);
                } else {
                    // not match, do nothing
                }
            } else { // input side not outer
                if (matched) {
                    if (otherIsOuter) { // if other side is outer
                        if (outerRecord.numOfAssociations == 0
                                && !isSuppress) { // if the matched num in the matched rows == 0
                            // send -D[null+other]
                            outRow.setRowKind(RowKind.DELETE);
                            outputNullPadding(outerRecord.record, !inputIsLeft);
                        }
                        // otherState.update(other, old + 1)
                        otherSideStateView.updateNumOfAssociations(
                                outerRecord.record, outerRecord.numOfAssociations + 1);
                        // send +I[record+other]s
                        outRow.setRowKind(RowKind.INSERT);
                    } else {
                        // send +I/+U[record+other]s (using input RowKind)
                        outRow.setRowKind(inputRowKind);
                    }
                    output(input, outerRecord.record, inputIsLeft);
                }
                // skip when there is no matched rows on the other side
            }
        } else { // input record is retract
            if (matched) {
                // there are matched rows on the other side
                if (inputIsOuter) {
                    // send -D[record+other]s
                    outRow.setRowKind(RowKind.DELETE);
                } else {
                    // send -D/-U[record+other]s (using input RowKind)
                    outRow.setRowKind(inputRowKind);
                }
                output(input, outerRecord.record, inputIsLeft);
                // if other side is outer
                if (otherIsOuter) {
                    if (outerRecord.numOfAssociations == 1 && !isSuppress) {
                        // send +I[null+other]
                        outRow.setRowKind(RowKind.INSERT);
                        outputNullPadding(outerRecord.record, !inputIsLeft);
                    } // nothing else to do when number of associations > 1
                    // otherState.update(other, old - 1)
                    otherSideStateView.updateNumOfAssociations(
                            outerRecord.record, outerRecord.numOfAssociations - 1);
                }
            }
        }
    }

    private void updateInputSideOnFinishIter(
            boolean isAccumulateMsg,
            boolean inputIsOuter,
            boolean isSuppress,
            boolean inputIsLeft,
            int matchedNum,
            RowData input,
            AsyncJoinRecordStateView inputSideStateView)
            throws Exception {
        if (isAccumulateMsg) {
            if (inputIsOuter) {
                if (matchedNum == 0) {
                    // send +I[record+null]
                    outRow.setRowKind(RowKind.INSERT);
                    outputNullPadding(input, inputIsLeft);
                    // state.add(record, 0)
                    inputSideStateView.addRecord(input, 0);
                } else {
                    // state.add(record, other.size)
                    inputSideStateView.addRecord(input, matchedNum);
                }
            } else {
                inputSideStateView.addRecord(input);
            }
        } else { // input record is retract
            if (!isSuppress) {
                inputSideStateView.retractRecord(input);
            }
            if (matchedNum == 0) { // there is no matched rows on the other side
                if (inputIsOuter) { // input side is outer
                    // send -D[record+null]
                    outRow.setRowKind(RowKind.DELETE);
                    outputNullPadding(input, inputIsLeft);
                }
            }
        }
    }

    private void output(RowData inputRow, RowData otherRow, boolean inputIsLeft) {
        if (inputIsLeft) {
            outRow.replace(inputRow, otherRow);
        } else {
            outRow.replace(otherRow, inputRow);
        }
        collector.collect(outRow);
    }

    private void outputNullPadding(RowData row, boolean isLeft) {
        if (isLeft) {
            outRow.replace(row, rightNullRow);
        } else {
            outRow.replace(leftNullRow, row);
        }
        collector.collect(outRow);
    }
}
