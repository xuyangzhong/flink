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

package org.apache.flink.table.planner.plan.rules.physical.stream;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DeltaJoinSpec;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalDeltaJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.optimize.program.FlinkChangelogModeInferenceProgram;
import org.apache.flink.table.planner.plan.optimize.program.StreamOptimizeContext;
import org.apache.flink.table.planner.plan.trait.UpdateKindTrait;
import org.apache.flink.table.planner.plan.utils.DeltaJoinUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.types.RowKind;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.hint.RelHint;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.plan.utils.DeltaJoinUtil.getChangelogMode;
import static org.apache.flink.table.planner.plan.utils.DeltaJoinUtil.getDeltaJoinSpec;
import static org.apache.flink.table.planner.plan.utils.DeltaJoinUtil.getTable;
import static org.apache.flink.table.planner.plan.utils.DeltaJoinUtil.getTableScan;
import static org.apache.flink.table.planner.plan.utils.DeltaJoinUtil.printPlan;
import static org.apache.flink.table.planner.plan.utils.DeltaJoinUtil.validateJoinKey;
import static org.apache.flink.table.planner.plan.utils.DeltaJoinUtil.validateSatisfyChangelog;
import static org.apache.flink.table.planner.plan.utils.DeltaJoinUtil.validateSupportedJoinType;
import static org.apache.flink.table.planner.plan.utils.DeltaJoinUtil.validateTableSource;
import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapContext;
import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig;

@Value.Enclosing
public class StreamPhysicalDeltaJoinRewriteRule
        extends RelRule<
                StreamPhysicalDeltaJoinRewriteRule.StreamPhysicalDeltaJoinRewriteRuleConfig> {

    public static final StreamPhysicalDeltaJoinRewriteRule INSTANCE =
            StreamPhysicalDeltaJoinRewriteRuleConfig.DEFAULT.toRule();

    private StreamPhysicalDeltaJoinRewriteRule(StreamPhysicalDeltaJoinRewriteRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        TableConfig tableConfig = unwrapTableConfig(call);
        OptimizerConfigOptions.DeltaJoinStrategy deltaJoinStrategy =
                tableConfig.get(OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY);
        return OptimizerConfigOptions.DeltaJoinStrategy.NONE != deltaJoinStrategy;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        StreamPhysicalJoin join = call.rel(0);
        TableConfig tableConfig = unwrapTableConfig(call);
        OptimizerConfigOptions.DeltaJoinStrategy deltaJoinStrategy =
                tableConfig.get(OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY);
        StreamOptimizeContext context = (StreamOptimizeContext) unwrapContext(call);
        try {
            convertToDeltaJoin(call, join, context);
        } catch (Throwable e) {
            if (deltaJoinStrategy == OptimizerConfigOptions.DeltaJoinStrategy.FORCE) {
                throw new TableException(e.getMessage(), e);
            }
        }
    }

    private void convertToDeltaJoin(
            RelOptRuleCall call, StreamPhysicalJoin join, StreamOptimizeContext context) {
        validateSupportedJoinType(join.getJoinType());

        UnwrapShuttle shuttle = new UnwrapShuttle();
        join = (StreamPhysicalJoin) join.accept(shuttle);

        List<RelNode> newInputs = tryUpdateInputTraitWithNewUpdateKind(join.getInputs(), context);

        newInputs.forEach(DeltaJoinUtil::validateNodeSupported);

        List<ChangelogMode> inputChangelogModes =
                newInputs.stream()
                        .map(input -> DeltaJoinUtil.getChangelogMode(unwrapNode(input)))
                        .collect(Collectors.toList());

        validateSatisfyChangelog(join, newInputs, inputChangelogModes);

        RelOptTable leftLookupTable = getTable(join.getLeft());
        RelOptTable rightLookupTable = getTable(join.getRight());

        List<Boolean> inputContainsUpdate =
                inputChangelogModes.stream()
                        .map(changelogMode -> changelogMode.contains(RowKind.UPDATE_AFTER))
                        .collect(Collectors.toList());

        validateJoinKey(
                JoinInfo.of(join.getLeft(), join.getRight(), join.getCondition()),
                newInputs,
                inputContainsUpdate);

        // get the spec that treats right table as lookup table
        DeltaJoinSpec leftDeltaJoinSpec =
                getDeltaJoinSpec(
                        join.getCondition(),
                        join.getLeft(),
                        leftLookupTable,
                        join.getRight(),
                        rightLookupTable,
                        join.getCluster(),
                        true);

        // get the spec that treats left table as lookup table
        DeltaJoinSpec rightDeltaJoinSpec =
                getDeltaJoinSpec(
                        join.getCondition(),
                        join.getLeft(),
                        leftLookupTable,
                        join.getRight(),
                        rightLookupTable,
                        join.getCluster(),
                        false);

        validateTableSource(rightLookupTable, leftDeltaJoinSpec.getLookupKeyMap().keySet());
        validateTableSource(leftLookupTable, rightDeltaJoinSpec.getLookupKeyMap().keySet());

        StreamPhysicalDeltaJoin deltaJoin =
                new StreamPhysicalDeltaJoin(
                        join.getCluster(),
                        join.getTraitSet(),
                        join.getHints(),
                        newInputs.get(0),
                        newInputs.get(1),
                        join.getCondition(),
                        join.getJoinType(),
                        leftLookupTable,
                        leftDeltaJoinSpec,
                        convertRelHintToMap(getTableScan(join.getLeft()).getHints()),
                        rightLookupTable,
                        rightDeltaJoinSpec,
                        convertRelHintToMap(getTableScan(join.getRight()).getHints()),
                        join.getRowType());

        call.transformTo(deltaJoin);
    }

    private static Map<String, String> convertRelHintToMap(List<RelHint> hints) {
        Map<String, String> map = new HashMap<>();
        for (RelHint hint : hints) {
            map.putAll(hint.kvOptions);
        }
        return map;
    }

    private List<RelNode> tryUpdateInputTraitWithNewUpdateKind(
            List<RelNode> originalInputs, StreamOptimizeContext context) {
        List<RelNode> newInputs = new ArrayList<>(originalInputs.size());
        for (RelNode input : originalInputs) {
            if (!getChangelogMode(unwrapNode(input)).contains(RowKind.UPDATE_BEFORE)) {
                newInputs.add(input);
                continue;
            }
            FlinkChangelogModeInferenceProgram.SatisfyUpdateKindTraitVisitor visitor =
                    new FlinkChangelogModeInferenceProgram.SatisfyUpdateKindTraitVisitor(context);
            Optional<StreamPhysicalRel> newInputOp =
                    JavaScalaConversionUtil.toJava(
                            visitor.visit(unwrapNode(input), UpdateKindTrait.ONLY_UPDATE_AFTER()));
            if (newInputOp.isEmpty()) {
                throw new DeltaJoinUtil.UnsupportedDeltaJoinOptimizationException(
                        String.format(
                                "Unsupported changelog mode UB encountered from one input of the join.\n"
                                        + "The input is:\n%s",
                                printPlan(input, true)));
            }
            newInputs.add(newInputOp.get());
        }
        return newInputs;
    }

    private static StreamPhysicalRel unwrapNode(RelNode relNode) {
        if (relNode instanceof StreamPhysicalRel) {
            return (StreamPhysicalRel) relNode;
        }
        if (relNode instanceof HepRelVertex) {
            return unwrapNode(((HepRelVertex) relNode).getCurrentRel());
        }
        throw new TableException("Unexpected exception: unknown node type !" + relNode.getClass());
    }

    private static class UnwrapShuttle extends RelHomogeneousShuttle {
        @Override
        public RelNode visit(RelNode node) {
            return super.visit(unwrapNode(node));
        }
    }

    @Value.Immutable
    public interface StreamPhysicalDeltaJoinRewriteRuleConfig extends RelRule.Config {
        StreamPhysicalDeltaJoinRewriteRule.StreamPhysicalDeltaJoinRewriteRuleConfig DEFAULT =
                ImmutableStreamPhysicalDeltaJoinRewriteRule.StreamPhysicalDeltaJoinRewriteRuleConfig
                        .builder()
                        .operandSupplier(b0 -> b0.operand(StreamPhysicalJoin.class).anyInputs())
                        .build();

        @Override
        default StreamPhysicalDeltaJoinRewriteRule toRule() {
            return new StreamPhysicalDeltaJoinRewriteRule(this);
        }
    }
}
