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

package org.apache.flink.table.planner.plan.nodes.physical.stream;

import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DeltaJoinSpec;
import org.apache.flink.table.planner.plan.utils.ExpressionFormat;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainLevel;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import scala.collection.JavaConverters;

import static org.apache.flink.table.planner.plan.utils.DeltaJoinUtil.lookupKeysToString;

/** Stream physical RelNode for delta join. */
public class StreamPhysicalDeltaJoin extends BiRel implements StreamPhysicalRel, Hintable {

    // ===== common =====
    private final RexNode condition;

    private final JoinRelType joinType;

    private final com.google.common.collect.ImmutableList<RelHint> hints;

    private final RelDataType rowType;

    // ===== related LEFT side =====

    private final RelOptTable leftLookupTable;

    // treat right side as lookup table
    private final DeltaJoinSpec leftDeltaJoinSpec;

    private final Map<String, String> leftExtraOptions;

    // ===== related RIGHT side =====

    private final RelOptTable rightLookupTable;

    // treat left side as lookup table
    private final DeltaJoinSpec rightDeltaJoinSpec;

    private final Map<String, String> rightExtraOptions;

    public StreamPhysicalDeltaJoin(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelHint> hints,
            RelNode left,
            RelNode right,
            RexNode condition,
            JoinRelType joinType,
            RelOptTable leftLookupTable,
            DeltaJoinSpec leftDeltaJoinSpec,
            Map<String, String> leftExtraOptions,
            RelOptTable rightLookupTable,
            DeltaJoinSpec rightDeltaJoinSpec,
            Map<String, String> rightExtraOptions,
            RelDataType rowType) {
        super(cluster, traitSet, left, right);
        this.hints = com.google.common.collect.ImmutableList.copyOf(hints);
        this.condition = condition;
        this.joinType = joinType;
        this.leftLookupTable = leftLookupTable;
        this.leftDeltaJoinSpec = leftDeltaJoinSpec;
        this.leftExtraOptions = leftExtraOptions;
        this.rightLookupTable = rightLookupTable;
        this.rightDeltaJoinSpec = rightDeltaJoinSpec;
        this.rightExtraOptions = rightExtraOptions;
        this.rowType = rowType;
    }

    @Override
    public ExecNode<?> translateToExecNode() {
        throw new UnsupportedOperationException("Introduce delta join exec node later");
    }

    @Override
    public boolean requireWatermark() {
        return false;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.size() == 2;
        return new StreamPhysicalDeltaJoin(
                getCluster(),
                traitSet,
                hints,
                inputs.get(0),
                inputs.get(1),
                condition,
                joinType,
                leftLookupTable,
                leftDeltaJoinSpec,
                leftExtraOptions,
                rightLookupTable,
                rightDeltaJoinSpec,
                rightExtraOptions,
                rowType);
    }

    @Override
    protected RelDataType deriveRowType() {
        return rowType;
    }

    @Override
    public com.google.common.collect.ImmutableList<RelHint> getHints() {
        return hints;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("joinType", joinType)
                .item(
                        "where",
                        conditionToString(condition, rowType.getFieldNames(), pw.getDetailLevel()))
                .item(
                        "rightLookupKeys",
                        lookupKeysToString(
                                leftDeltaJoinSpec.getLookupKeyMap(),
                                rightLookupTable.getRowType().getFieldNames(),
                                leftLookupTable.getRowType().getFieldNames()))
                .itemIf(
                        "rightRemainingCondition",
                        leftDeltaJoinSpec
                                .getRemainingCondition()
                                .map(
                                        cond ->
                                                conditionToString(
                                                        cond,
                                                        rowType.getFieldNames(),
                                                        pw.getDetailLevel()))
                                .orElse(null),
                        leftDeltaJoinSpec.getRemainingCondition().isPresent())
                .item(
                        "leftLookupKeys",
                        lookupKeysToString(
                                rightDeltaJoinSpec.getLookupKeyMap(),
                                leftLookupTable.getRowType().getFieldNames(),
                                rightLookupTable.getRowType().getFieldNames()))
                .itemIf(
                        "leftRemainingCondition",
                        rightDeltaJoinSpec
                                .getRemainingCondition()
                                .map(
                                        cond ->
                                                conditionToString(
                                                        cond,
                                                        rowType.getFieldNames(),
                                                        pw.getDetailLevel()))
                                .orElse(null),
                        rightDeltaJoinSpec.getRemainingCondition().isPresent())
                .item("select", String.join(", ", rowType.getFieldNames()));
    }

    private String conditionToString(
            RexNode condition, List<String> outputFieldNames, SqlExplainLevel sqlExplainLevel) {
        return getExpressionString(
                condition,
                JavaConverters.asScalaBufferConverter(outputFieldNames).asScala().toList(),
                JavaScalaConversionUtil.toScala(Optional.empty()),
                ExpressionFormat.Infix(),
                sqlExplainLevel);
    }
}
