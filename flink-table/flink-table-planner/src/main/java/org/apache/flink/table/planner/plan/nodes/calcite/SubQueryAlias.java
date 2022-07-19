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

package org.apache.flink.table.planner.plan.nodes.calcite;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;

/**
 * Relational operator that records the names of the sub-query.
 *
 * <p>This operator is not implements from {@link org.apache.calcite.rel.hint.Hintable} so it can
 * avoid propagating hint into sub-query.
 */
public abstract class SubQueryAlias extends SingleRel {

    protected final String aliasName;

    protected SubQueryAlias(
            RelOptCluster cluster, RelTraitSet traits, RelNode input, String aliasName) {
        super(cluster, traits, input);
        this.aliasName = aliasName;
    }

    public String getAliasName() {
        return aliasName;
    }
}
