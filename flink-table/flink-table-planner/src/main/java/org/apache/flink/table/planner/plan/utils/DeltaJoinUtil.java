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

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.Index;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DeltaJoinSpec;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExchange;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWatermarkAssigner;
import org.apache.flink.table.planner.plan.schema.IntermediateRelTable;
import org.apache.flink.table.planner.plan.schema.LegacyTableSourceTable;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava33.com.google.common.collect.Sets;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.table.planner.plan.utils.LookupJoinUtil.isAsyncLookup;

public class DeltaJoinUtil {

    private static final String NON_LOOKUP_TABLE_ERROR_MSG_TEMPLATE =
            "Table [%s] does not implement LookupTableSource interface.";

    private static final String NON_ASYNC_LOOKUP_TABLE_ERROR_MSG_TEMPLATE =
            "Table [%s] does not support async lookup.";

    private static final String NON_ASYNC_LOOKUP_TABLE_SOLUTION_MSG =
            "If the table supports the option of async lookup joins, "
                    + "add it to the with parameters of the DDL.";

    private static final Set<JoinRelType> ALL_SUPPORTED_JOIN_TYPES =
            Sets.newHashSet(JoinRelType.INNER);

    /**
     * All allowed delta join upstream nodes. Only the following nodes are allowed to exist between
     * the delta join and the source. Otherwise, an {@link
     * UnsupportedDeltaJoinOptimizationException} will be thrown.
     *
     * <p>More physical nodes can be added to support more patterns for delta join.
     */
    private static final Set<Class<?>> ALL_ALLOWED_DELTA_JOIN_UPSTREAM_NODES =
            Sets.newHashSet(StreamPhysicalTableSourceScan.class, StreamPhysicalExchange.class);

    /** Check if the node is supported during delta join optimization. */
    public static void validateNodeSupported(RelNode node) {
        Class<?> nodeClazz = node.getClass();
        boolean isSupported = ALL_ALLOWED_DELTA_JOIN_UPSTREAM_NODES.contains(nodeClazz);
        if (!isSupported) {
            throw new UnsupportedDeltaJoinOptimizationException(
                    String.format(
                            "Unsupported node [%s] encountered upstream of this join node while attempting to "
                                    + "optimize to delta join.\n"
                                    + "The unsupported node and its inputs is:\n%s",
                            nodeClazz.getSimpleName(), printPlan(node, false)));
        }
    }

    public static void validateSatisfyChangelog(
            StreamPhysicalJoin join,
            List<RelNode> newInputs,
            List<ChangelogMode> inputChangelogModes) {
        // if changelog modes of all inputs are [I] or [I,UA], pass
        if (inputChangelogModes.stream()
                .allMatch(
                        changelogMode ->
                                !changelogMode.contains(RowKind.DELETE)
                                        && !changelogMode.contains(RowKind.UPDATE_BEFORE))) {
            return;
        }

        // if the changelog mode of any input contains UB, fail
        for (int i = 0; i < inputChangelogModes.size(); i++) {
            ChangelogMode inputChangelogMode = inputChangelogModes.get(i);
            if (inputChangelogMode.contains(RowKind.UPDATE_BEFORE)) {
                throw new DeltaJoinUtil.UnsupportedDeltaJoinOptimizationException(
                        String.format(
                                "Unsupported changelog mode UB encountered from one input of the join.\n"
                                        + "The input is:\n%s",
                                printPlan(newInputs.get(i), true)));
            }
        }

        JoinRelType joinType = join.getJoinType();
        Preconditions.checkArgument(ALL_SUPPORTED_JOIN_TYPES.contains(joinType));

        // if the changelog mode of any input contains D:
        // 1. check each join type:
        //  1.1 for inner join, if only one side producing D, pass; else fail
        //  1.2 left/right/full is not supported yet, fail
        // 2. check if the join support key-only-deletes, pass; else fail
        if (JoinRelType.INNER == joinType) {
            if (inputChangelogModes.stream()
                    .allMatch(changelogMode -> changelogMode.contains(RowKind.DELETE))) {
                throw new UnsupportedDeltaJoinOptimizationException(
                        String.format(
                                "Unsupported changelog mode D encountered from both inputs of the join.\n"
                                        + "The left input is:\n%s\n"
                                        + "The right input is:\n%s",
                                printPlan(newInputs.get(0), true),
                                printPlan(newInputs.get(1), true)),
                        "For inner join, only the join with only one input producing D can be optimized to delta join");
            }
        } else {
            throw new UnsupportedDeltaJoinOptimizationException(
                    String.format(
                            "Unsupported join type [%s] encountered while attempting to convert to delta join.",
                            joinType));
        }
        ChangelogMode joinChangelogMode = getChangelogMode(join);
        if (!joinChangelogMode.keyOnlyDeletes()) {
            throw new UnsupportedDeltaJoinOptimizationException(
                    "Unsupported join changelog without key-only-deletes encountered.",
                    "If any input of the join can produce D, only the join that satisfies "
                            + "delete-by-key can be optimized to delta join ");
        }
    }

    public static ChangelogMode getChangelogMode(StreamPhysicalRel node) {
        return JavaScalaConversionUtil.toJava(ChangelogPlanUtils.getChangelogMode(node))
                .orElseThrow(
                        () -> new TableException("Could not get the change mode from the node"));
    }

    /** Check if the join type is supported during delta join optimization. */
    public static void validateSupportedJoinType(JoinRelType joinType) {
        if (!ALL_SUPPORTED_JOIN_TYPES.contains(joinType)) {
            throw new UnsupportedDeltaJoinOptimizationException(
                    String.format(
                            "Unsupported join type [%s] encountered while attempting to convert to delta join.",
                            joinType));
        }
    }

    public static RelOptTable getTable(RelNode node) {
        return getTableScan(node).getTable();
    }

    public static TableScan getTableScan(RelNode node) {
        // support to get table across more nodes if we support more nodes in
        // `ALL_ALLOWED_DELTA_JOIN_UPSTREAM_NODES`
        if (node instanceof StreamPhysicalExchange) {
            return getTableScan(((StreamPhysicalExchange) node).getInput());
        }
        if (node instanceof StreamPhysicalWatermarkAssigner) {
            return getTableScan(((StreamPhysicalWatermarkAssigner) node).getInput());
        }
        if (!(node instanceof TableScan)) {
            throw new UnsupportedDeltaJoinOptimizationException(
                    "Failed to get table from node [%s].", node.getClass().getSimpleName());
        }
        return (TableScan) node;
    }

    public static void validateTableSource(RelOptTable table, Collection<Integer> lookupKeys) {
        TableSourceTable tableSourceTable = getTableSourceTable(table);
        // the source must be able to be used as a lookup table
        if (!(tableSourceTable.tableSource() instanceof LookupTableSource)) {
            throw new UnsupportedDeltaJoinOptimizationException(
                    String.format(
                            NON_LOOKUP_TABLE_ERROR_MSG_TEMPLATE,
                            StringUtils.join(table.getQualifiedName(), ".")));
        }
        // the source must be an async lookup source
        if (!isAsyncLookup(table, lookupKeys, null, false, false)) {
            throw new UnsupportedDeltaJoinOptimizationException(
                    String.format(
                            NON_ASYNC_LOOKUP_TABLE_ERROR_MSG_TEMPLATE,
                            StringUtils.join(table.getQualifiedName(), ".")),
                    NON_ASYNC_LOOKUP_TABLE_SOLUTION_MSG);
        }
    }

    public static void validateJoinKey(
            JoinInfo joinInfo, List<RelNode> newInputs, List<Boolean> inputContainsUpdate) {
        Preconditions.checkArgument(newInputs.size() == 2);
        Preconditions.checkArgument(inputContainsUpdate.size() == 2);

        // if both side do not contain update, we can skip the validation
        if (inputContainsUpdate.stream().noneMatch(Boolean::booleanValue)) {
            return;
        }

        List<IntPair> joinKeys = joinInfo.pairs();

        ImmutableBitSet leftJoinKeyRefs =
                ImmutableBitSet.of(
                        IntPair.left(joinKeys).stream().mapToInt(Integer::intValue).toArray());
        ImmutableBitSet rightJoinKeyRefs =
                ImmutableBitSet.of(
                        IntPair.right(joinKeys).stream().mapToInt(Integer::intValue).toArray());

        validateJoinKey(leftJoinKeyRefs, newInputs.get(0), inputContainsUpdate.get(0));
        validateJoinKey(rightJoinKeyRefs, newInputs.get(1), inputContainsUpdate.get(1));
    }

    private static void validateJoinKey(
            ImmutableBitSet joinKeyRefs, RelNode input, boolean containsUpdate) {
        if (!containsUpdate) {
            return;
        }
        RelOptTable table = getTable(input);
        // TODO support calc to re-map the pk
        ImmutableBitSet pk = getPrimaryKeyIndexesOfTemporalTable(table);
        if (pk.isEmpty()) {
            throw new UnsupportedDeltaJoinOptimizationException(
                    String.format(
                            "Unsupported table source [%s] for delta join because it has no primary keys "
                                    + "but will produce update messages.",
                            StringUtils.join(table.getQualifiedName(), ".")));
        }

        if (!pk.contains(joinKeyRefs)) {
            throw new UnsupportedOperationException(
                    "Unsupported join key because it neither be a primary key or a part of a "
                            + "composite primary key while the input changelog contains update.");
        }
    }

    /** Print all lookup keys as readable string. */
    public static String lookupKeysToString(
            Map<Integer, LookupJoinUtil.LookupKey> lookupKeyMap,
            List<String> lookupTableFields,
            List<String> nonLookupTableFields) {
        return lookupKeyMap.entrySet().stream()
                .map(
                        entry -> {
                            String template = "%s=%s";
                            if (entry.getValue() instanceof LookupJoinUtil.FieldRefLookupKey) {
                                LookupJoinUtil.FieldRefLookupKey fieldKey =
                                        (LookupJoinUtil.FieldRefLookupKey) entry.getValue();
                                return String.format(
                                        template,
                                        lookupTableFields.get(entry.getKey()),
                                        nonLookupTableFields.get(fieldKey.index));
                            } else if (entry.getValue()
                                    instanceof LookupJoinUtil.ConstantLookupKey) {
                                LookupJoinUtil.ConstantLookupKey constantKey =
                                        (LookupJoinUtil.ConstantLookupKey) entry.getValue();
                                return String.format(
                                        template,
                                        lookupTableFields.get(entry.getKey()),
                                        RelExplainUtil.literalToString(constantKey.literal));
                            } else {
                                throw new TableException(
                                        "Unsupported lookup key type: "
                                                + entry.getValue().getClass().getSimpleName());
                            }
                        })
                .collect(java.util.stream.Collectors.joining(", "));
    }

    /** Get the delta join spec of the temporal table. */
    public static DeltaJoinSpec getDeltaJoinSpec(
            RexNode joinCondition,
            RelNode leftInput,
            RelOptTable leftTable,
            RelNode rightInput,
            RelOptTable rightTable,
            RelOptCluster cluster,
            boolean treatRightTableAsLookupTable) {
        Map<Integer, LookupJoinUtil.LookupKey> allLookupKeys;
        Optional<RexNode> remainingCondition;
        JoinInfo joinInfo = JoinInfo.of(leftInput, rightInput, joinCondition);

        if (joinInfo.pairs().isEmpty()) {
            throw new UnsupportedDeltaJoinOptimizationException(
                    "There must be at least one equality condition between a field in the left table and a field in the right table as the join condition.");
        }

        List<IntPair> joinKeys = joinInfo.pairs();
        if (treatRightTableAsLookupTable) {
            allLookupKeys = analyzerDeltaJoinLookupKeys(joinKeys, rightTable, leftTable, true);
            remainingCondition =
                    getRemainingJoinCondition(
                            cluster.getRexBuilder(),
                            rightTable.getRowType(),
                            leftTable.getRowType(),
                            new ArrayList<>(allLookupKeys.values()),
                            joinKeys,
                            joinInfo.nonEquiConditions,
                            true);
        } else {
            List<IntPair> reversedJoinKeys = reverseIntPairs(joinKeys);
            allLookupKeys =
                    analyzerDeltaJoinLookupKeys(reversedJoinKeys, leftTable, rightTable, false);
            remainingCondition =
                    getRemainingJoinCondition(
                            cluster.getRexBuilder(),
                            leftTable.getRowType(),
                            rightTable.getRowType(),
                            new ArrayList<>(allLookupKeys.values()),
                            reversedJoinKeys,
                            joinInfo.nonEquiConditions,
                            false);
        }

        return new DeltaJoinSpec(remainingCondition.orElse(null), allLookupKeys);
    }

    public static String printPlan(RelNode node, boolean withChangelogTraits) {
        return FlinkRelOptUtil.toString(
                node,
                SqlExplainLevel.DIGEST_ATTRIBUTES,
                false,
                withChangelogTraits,
                false,
                false,
                false);
    }

    private static List<IntPair> reverseIntPairs(List<IntPair> intPairs) {
        return intPairs.stream()
                .map(pair -> new IntPair(pair.target, pair.source))
                .collect(Collectors.toList());
    }

    /**
     * Gets the remaining join condition which is used.
     *
     * <p>Similar to {@code LookupJoinUtil#getRemainingJoinCondition}, But in a delta join, it is
     * slightly different:
     *
     * <ol>
     *   <li>Delta join only supports {@link LookupJoinUtil.FieldRefLookupKey} in lookup keys.
     *   <li>both the left table and the right table can be lookup tables, and both need to be
     *       analyzed.
     * </ol>
     *
     * @param rexBuilder the rex builder
     * @param lookupTableRelDataType if we are analyzing right loop up table, this arg is the data
     *     type of the right lookup table; else it is the data type of the left lookup table
     * @param nonLookupTableRelDataType if we are analyzing right loop up table, this arg is the
     *     data type of the left lookup table; else it is the data type of the right lookup table
     * @param nonLookupTableKeys the lookup keys of the non lookup table. If we are analyzing right
     *     loop up table, this arg is the lookup keys of the left lookup table; else it is the
     *     lookup keys of the right lookup table
     * @param joinKeys the join key pairs. If we are analyzing right loop up table, we have reversed
     *     the left side and right side in these pairs.
     * @param nonEquiConditions the non-equi conditions in join
     * @param isAnalyzingRightLookupTable whether we are analyzing right loop up table
     */
    private static Optional<RexNode> getRemainingJoinCondition(
            RexBuilder rexBuilder,
            RelDataType lookupTableRelDataType,
            RelDataType nonLookupTableRelDataType,
            List<LookupJoinUtil.LookupKey> nonLookupTableKeys,
            List<IntPair> joinKeys,
            List<RexNode> nonEquiConditions,
            boolean isAnalyzingRightLookupTable) {
        // fields indices of the non lookup table side about join keys
        List<Integer> keyIndexesOfNonLookupTable =
                nonLookupTableKeys.stream()
                        .filter(k -> k instanceof LookupJoinUtil.FieldRefLookupKey)
                        .map(k -> ((LookupJoinUtil.FieldRefLookupKey) k).index)
                        .collect(Collectors.toList());
        // the key index of lookup table may be duplicated in joinPairs,
        // we should filter the key-pair by checking key index of the non lookup table.
        List<IntPair> remainingPairs =
                joinKeys.stream()
                        .filter(p -> !keyIndexesOfNonLookupTable.contains(p.source))
                        .collect(Collectors.toList());
        // convert remaining pairs to RexInputRef tuple for building SqlStdOperatorTable.EQUALS
        // calls
        List<RexNode> remainingEquals =
                remainingPairs.stream()
                        .map(
                                intPair -> {
                                    RexInputRef inputRefOnNonLookupTable;
                                    RexInputRef inputRefOnLookupTable;
                                    if (isAnalyzingRightLookupTable) {
                                        // right side is the lookup table
                                        RelDataTypeField fieldTypeOnNonLookupTable =
                                                nonLookupTableRelDataType
                                                        .getFieldList()
                                                        .get(intPair.source);
                                        inputRefOnNonLookupTable =
                                                new RexInputRef(
                                                        intPair.source,
                                                        fieldTypeOnNonLookupTable.getType());

                                        RelDataTypeField fieldTypeOnLookupTable =
                                                lookupTableRelDataType
                                                        .getFieldList()
                                                        .get(intPair.target);

                                        inputRefOnLookupTable =
                                                new RexInputRef(
                                                        nonLookupTableRelDataType.getFieldCount()
                                                                + intPair.target,
                                                        fieldTypeOnLookupTable.getType());
                                    } else {
                                        // left side is the lookup table
                                        RelDataTypeField fieldTypeOnNonLookupTable =
                                                nonLookupTableRelDataType
                                                        .getFieldList()
                                                        .get(intPair.source);
                                        inputRefOnNonLookupTable =
                                                new RexInputRef(
                                                        lookupTableRelDataType.getFieldCount()
                                                                + intPair.source,
                                                        fieldTypeOnNonLookupTable.getType());

                                        RelDataTypeField fieldTypeOnLookupTable =
                                                lookupTableRelDataType
                                                        .getFieldList()
                                                        .get(intPair.target);

                                        inputRefOnLookupTable =
                                                new RexInputRef(
                                                        intPair.target,
                                                        fieldTypeOnLookupTable.getType());
                                    }

                                    return rexBuilder.makeCall(
                                            SqlStdOperatorTable.EQUALS,
                                            inputRefOnNonLookupTable,
                                            inputRefOnLookupTable);
                                })
                        .collect(Collectors.toList());

        List<RexNode> remainingAnds =
                Stream.concat(remainingEquals.stream(), nonEquiConditions.stream())
                        .collect(Collectors.toList());
        // build a new condition
        RexNode condition = RexUtil.composeConjunction(rexBuilder, remainingAnds);
        if (condition.isAlwaysTrue()) {
            return Optional.empty();
        } else {
            return Optional.of(condition);
        }
    }

    /**
     * Analyze potential lookup keys of the temporal table from the join condition on the temporal
     * table for delta join.
     *
     * @param joinKeyPairs all join keys. The target of the {@link IntPair#target} represents the
     *     analyzed side
     * @param lookupTable the temporal table on the lookup side
     * @param nonLookupTable the temporal table on the non-lookup side
     */
    private static Map<Integer, LookupJoinUtil.LookupKey> analyzerDeltaJoinLookupKeys(
            List<IntPair> joinKeyPairs,
            RelOptTable lookupTable,
            RelOptTable nonLookupTable,
            boolean treatRightTableAsLookupTable) {
        Map<Integer, LookupJoinUtil.LookupKey> fieldRefLookupKeys = new LinkedHashMap<>();
        for (IntPair intPair : joinKeyPairs) {
            fieldRefLookupKeys.put(
                    intPair.target, new LookupJoinUtil.FieldRefLookupKey(intPair.source));
        }

        // 2. use indexes as lookup keys
        Optional<Map<Integer, LookupJoinUtil.LookupKey>> lookupKeysThatAreIndex =
                tryGetLookupKeysFromIndexes(lookupTable, fieldRefLookupKeys);

        if (lookupKeysThatAreIndex.isPresent()) {
            return lookupKeysThatAreIndex.get();
        }

        // build readable error message
        String tableName = StringUtils.join(lookupTable.getQualifiedName(), ".");

        List<List<String>> allFieldsAboutAllIndexes =
                getAllIndexesColumnsOfTemporaryTable(lookupTable);

        StringBuilder sb = new StringBuilder();
        sb.append(
                String.format(
                        "The join key [%s] does not include"
                                + "all fields from any index on the %s table [%s].\n",
                        lookupKeysToString(
                                fieldRefLookupKeys,
                                lookupTable.getRowType().getFieldNames(),
                                nonLookupTable.getRowType().getFieldNames()),
                        treatRightTableAsLookupTable ? "right" : "left",
                        tableName));

        String indexesStr;
        if (allFieldsAboutAllIndexes.isEmpty()) {
            indexesStr = "N/A";
        } else {
            indexesStr =
                    allFieldsAboutAllIndexes.stream()
                            .map(
                                    fields ->
                                            fields.stream()
                                                    .collect(Collectors.joining(", ", "(", ")")))
                            .collect(Collectors.joining(", ", "[", "]"));
        }
        sb.append(String.format("All indexes of the table [%s] is %s.", tableName, indexesStr));

        throw new UnsupportedDeltaJoinOptimizationException(sb.toString());
    }

    private static Optional<List<String>> getPrimaryKeyColumnsOfTemporalTable(
            RelOptTable temporalTable) {
        ResolvedSchema schema = null;
        if (temporalTable instanceof TableSourceTable) {
            schema = ((TableSourceTable) temporalTable).contextResolvedTable().getResolvedSchema();
        } else if (temporalTable instanceof LegacyTableSourceTable) {
            throw new UnsupportedDeltaJoinOptimizationException(
                    "Delta join does not supported the legacy table source [%s]",
                    ((LegacyTableSourceTable<?>) temporalTable)
                            .tableIdentifier()
                            .asSummaryString());
        } else if (temporalTable instanceof IntermediateRelTable) {
            IntermediateRelTable intermediateRelTable = (IntermediateRelTable) temporalTable;
            if (intermediateRelTable.relNode() instanceof TableScan) {
                TableScan tableScan = (TableScan) (intermediateRelTable.relNode());
                TableSourceTable table = (TableSourceTable) tableScan.getTable();
                schema = table.contextResolvedTable().getResolvedSchema();
            } else {
                throw new TableException(
                        "Unexpected exception: the node inside intermediate table must be a table source scan");
            }
        }

        if (schema != null) {
            Optional<UniqueConstraint> constraint = schema.getPrimaryKey();
            return constraint.map(UniqueConstraint::getColumns);
        } else {
            throw new TableException("Unexpected exception: unknown temporal table type !");
        }
    }

    private static ImmutableBitSet getPrimaryKeyIndexesOfTemporalTable(RelOptTable temporalTable) {
        // get primary key columns of lookup table if exists
        Optional<List<String>> pkColumns = getPrimaryKeyColumnsOfTemporalTable(temporalTable);

        if (pkColumns.isPresent()) {
            List<String> newSchema = temporalTable.getRowType().getFieldNames();
            return ImmutableBitSet.of(
                    pkColumns.get().stream().mapToInt(newSchema::indexOf).toArray());
        } else {
            return ImmutableBitSet.of();
        }
    }

    private static Optional<Map<Integer, LookupJoinUtil.LookupKey>> tryGetLookupKeysFromIndexes(
            RelOptTable lookupTable, Map<Integer, LookupJoinUtil.LookupKey> fieldRefLookupKeys) {
        // get indexes of lookup table if exists
        int[][] idxsOfAllIndexes = getColumnIndicesOfAllTemporalTableIndexes(lookupTable);

        for (int[] idxsOfIndex : idxsOfAllIndexes) {
            Preconditions.checkState(idxsOfIndex.length > 0);

            // ignore the field order of the index
            boolean containsIndex =
                    Arrays.stream(idxsOfIndex).allMatch(fieldRefLookupKeys::containsKey);
            if (containsIndex) {
                // all fields with this index are included, and other fields are pruned
                Map<Integer, LookupJoinUtil.LookupKey> lookupKeysThatAreIndex =
                        fieldRefLookupKeys.entrySet().stream()
                                .filter(
                                        entry ->
                                                Arrays.stream(idxsOfIndex)
                                                        .anyMatch(idx -> idx == entry.getKey()))
                                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                return Optional.of(lookupKeysThatAreIndex);
            }
        }

        return Optional.empty();
    }

    public static int[][] getColumnIndicesOfAllTemporalTableIndexes(RelOptTable temporalTable) {
        List<List<String>> columnsOfIndexes = getAllIndexesColumnsOfTemporaryTable(temporalTable);
        int[][] results = new int[columnsOfIndexes.size()][];
        for (int i = 0; i < columnsOfIndexes.size(); i++) {
            List<String> fieldNames = temporalTable.getRowType().getFieldNames();
            results[i] = columnsOfIndexes.get(i).stream().mapToInt(fieldNames::indexOf).toArray();
        }

        return results;
    }

    private static List<List<String>> getAllIndexesColumnsOfTemporaryTable(
            RelOptTable temporalTable) {
        TableSourceTable tableSourceTable = getTableSourceTable(temporalTable);
        ResolvedSchema schema = tableSourceTable.contextResolvedTable().getResolvedSchema();
        List<Index> indexes = schema.getIndexes();
        return indexes.stream().map(Index::getColumns).collect(Collectors.toList());
    }

    private static TableSourceTable getTableSourceTable(RelOptTable table) {
        if (table instanceof TableSourceTable) {
            return (TableSourceTable) table;
        }
        if (table instanceof LegacyTableSourceTable) {
            throw new UnsupportedDeltaJoinOptimizationException(
                    String.format(
                            "Delta join does not supported the legacy table source [%s]",
                            ((LegacyTableSourceTable<?>) table)
                                    .tableIdentifier()
                                    .asSummaryString()));
        }
        throw new TableException(
                String.format(
                        "Unsupported temporal table type '%s'!", table.getClass().getSimpleName()));
    }

    /** Exception thrown when the current sql don't support to do delta join optimization. */
    public static class UnsupportedDeltaJoinOptimizationException extends TableException {
        private static final String ERROR_TEXT =
                "The current sql doesn't support to do delta join optimization.";

        public UnsupportedDeltaJoinOptimizationException(String reason) {
            super(getErrorMessage(reason, null));
        }

        public UnsupportedDeltaJoinOptimizationException(String reason, String solution) {
            super(getErrorMessage(reason, solution));
        }

        private static String getErrorMessage(@Nullable String reason, @Nullable String solution) {
            StringBuilder sb = new StringBuilder(ERROR_TEXT);
            if (reason != null) {
                sb.append("\n\nReason:\n\t").append(reason);
            }
            if (solution != null) {
                sb.append("\n\nSolution:\n\t").append(solution);
            }

            return sb.toString();
        }
    }
}
