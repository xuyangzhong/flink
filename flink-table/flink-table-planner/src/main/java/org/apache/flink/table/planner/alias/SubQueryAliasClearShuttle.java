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

package org.apache.flink.table.planner.alias;

import org.apache.flink.table.planner.hint.FlinkHints;
import org.apache.flink.table.planner.plan.nodes.calcite.SubQueryAlias;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;

import java.util.Collections;

/** Shuttle to remove sub-query alias node. */
public class SubQueryAliasClearShuttle extends RelShuttleImpl {
    @Override
    public RelNode visit(RelNode node) {
        if (node instanceof SubQueryAlias) {
            RelHint aliasTag =
                    RelHint.builder(FlinkHints.HINT_ALIAS)
                            .hintOption(((SubQueryAlias) node).getAliasName())
                            .build();
            RelNode newNode =
                    ((Hintable) ((SubQueryAlias) node).getInput())
                            .attachHints(Collections.singletonList(aliasTag));
            return super.visit(newNode);
        }

        return super.visit(node);
    }
}
