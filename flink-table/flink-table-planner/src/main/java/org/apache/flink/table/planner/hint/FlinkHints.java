/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.hint;

import org.apache.flink.table.planner.plan.rules.logical.WrapJsonAggFunctionArgumentsRule;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** Utility class for Flink hints. */
public abstract class FlinkHints {
    // ~ Static fields/initializers ---------------------------------------------

    public static final String HINT_NAME_OPTIONS = "OPTIONS";

    // ~ Internal alias tag hint
    public static final String HINT_ALIAS = "ALIAS";

    /**
     * Internal hint that JSON aggregation function arguments have been wrapped already. See {@link
     * WrapJsonAggFunctionArgumentsRule}.
     */
    public static final String HINT_NAME_JSON_AGGREGATE_WRAPPED = "JSON_AGGREGATE_WRAPPED";

    // ~ Tools ------------------------------------------------------------------

    /**
     * Returns the OPTIONS hint options from the given list of table hints {@code tableHints}, never
     * null.
     */
    public static Map<String, String> getHintedOptions(List<RelHint> tableHints) {
        return tableHints.stream()
                .filter(hint -> hint.hintName.equalsIgnoreCase(HINT_NAME_OPTIONS))
                .findFirst()
                .map(hint -> hint.kvOptions)
                .orElse(Collections.emptyMap());
    }

    /**
     * Merges the dynamic table options from {@code hints} and static table options from table
     * definition {@code props}.
     *
     * <p>The options in {@code hints} would override the ones in {@code props} if they have the
     * same option key.
     *
     * @param hints Dynamic table options, usually from the OPTIONS hint
     * @param props Static table options defined in DDL or connect API
     * @return New options with merged dynamic table options, or the old {@code props} if there is
     *     no dynamic table options
     */
    public static Map<String, String> mergeTableOptions(
            Map<String, String> hints, Map<String, String> props) {
        if (hints.size() == 0) {
            return props;
        }
        Map<String, String> newProps = new HashMap<>();
        newProps.putAll(props);
        newProps.putAll(hints);
        return Collections.unmodifiableMap(newProps);
    }

    public static Optional<String> getTableAlias(RelNode node) {
        if (node instanceof Hintable) {
            Hintable aliasNode = (Hintable) node;
            List<String> viewNames =
                    aliasNode.getHints().stream()
                            .filter(h -> h.hintName.equalsIgnoreCase(FlinkHints.HINT_ALIAS))
                            .flatMap(h -> h.listOptions.stream())
                            .collect(Collectors.toList());
            if (viewNames.size() > 0) {
                return Optional.of(viewNames.get(0));
            } else {
                if (canTransposeAliasHint(node)) {
                    return getTableAlias(node.getInput(0));
                }
            }
        }
        return Optional.empty();
    }

    private static boolean canTransposeAliasHint(RelNode node) {
        return node instanceof LogicalProject || node instanceof LogicalFilter;
    }

    /** Returns the qualified name of a table scan, otherwise returns empty. */
    public static Optional<String> getTableName(RelOptTable table) {
        if (table == null) {
            return Optional.empty();
        }
        List<String> qualifiedNames = table.getQualifiedName();
        return Optional.of(String.join(".", qualifiedNames));
    }

    public static String stringifyHints(List<RelHint> hints) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (RelHint h : hints) {
            if (h.hintName.equalsIgnoreCase(FlinkHints.HINT_ALIAS)) {
                continue;
            }
            if (!first) {
                sb.append(", ");
            }
            sb.append(h.hintName);
            if (h.listOptions.size() > 0) {
                sb.append("(").append(String.join(", ", h.listOptions)).append(")");
            } else if (h.kvOptions.size() > 0) {
                String mapStr =
                        h.kvOptions.entrySet().stream()
                                .map(e -> e.getKey() + "=" + e.getValue())
                                .collect(Collectors.joining(", "));
                sb.append("(").append(mapStr).append(")");
            }
            first = false;
        }
        return sb.toString();
    }

    public static boolean checkJoinHintsConflictBetweenNodes(List<RelNode> nodes) {
        List<RelHint> globalJoinHints = new ArrayList<>();
        for (RelNode node : nodes) {
            if (!(node instanceof Hintable)) {
                continue;
            }
            List<RelHint> hintsOnThisNode = ((Hintable) node).getHints();
            // only care about the join hints
            List<RelHint> nowJoinHint =
                    hintsOnThisNode.stream()
                            .filter(hint -> JoinStrategy.isJoinStrategy(hint.hintName))
                            .map(FlinkHints::ignoreInheritPath)
                            .collect(Collectors.toList());

            if (nowJoinHint.size() != 0) {
                // if the order is not same with global join hints, also consider as conflict
                for (int i = 0; i < Math.min(nowJoinHint.size(), globalJoinHints.size()); i++) {
                    if (!globalJoinHints.get(i).equals(nowJoinHint.get(i))) {
                        return false;
                    }
                }
                if (nowJoinHint.size() > globalJoinHints.size()) {
                    globalJoinHints = nowJoinHint;
                }
            }
        }
        return false;
    }

    private static RelHint ignoreInheritPath(RelHint hint) {
        RelHint.Builder builder = RelHint.builder(hint.hintName);
        if (!hint.listOptions.isEmpty()) {
            builder.hintOptions(hint.listOptions);
        } else {
            builder.hintOptions(hint.kvOptions);
        }
        return builder.build();
    }
}
