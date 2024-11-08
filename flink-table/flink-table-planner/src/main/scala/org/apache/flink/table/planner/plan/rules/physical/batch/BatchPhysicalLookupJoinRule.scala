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
package org.apache.flink.table.planner.plan.rules.physical.batch

import org.apache.flink.table.legacy.sources.LookupableTableSource
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical._
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalLookupJoin
import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalLookupJoin
import org.apache.flink.table.planner.plan.rules.physical.common.{BaseSnapshotOnCalcTableScanRule, BaseSnapshotOnTableScanRule}

import org.apache.calcite.plan.{RelOptRule, RelOptTable}
import org.apache.calcite.rex.RexProgram

import java.util

/**
 * Rules that convert [[FlinkLogicalJoin]] on a [[FlinkLogicalSnapshot]] into
 * [[BatchPhysicalLookupJoin]].
 *
 * There are 2 conditions for this rule:
 *   1. the root parent of [[FlinkLogicalSnapshot]] should be a TableSource which implements
 *      [[LookupableTableSource]]. 2. the period of [[FlinkLogicalSnapshot]] must be left table's
 *      proctime attribute.
 */
object BatchPhysicalLookupJoinRule {
  val SNAPSHOT_ON_TABLESCAN: RelOptRule = new SnapshotOnTableScanRule
  val SNAPSHOT_ON_CALC_TABLESCAN: RelOptRule = new SnapshotOnCalcTableScanRule

  class SnapshotOnTableScanRule
    extends BaseSnapshotOnTableScanRule("BatchPhysicalSnapshotOnTableScanRule") {

    override protected def transform(
        join: FlinkLogicalJoin,
        input: FlinkLogicalRel,
        temporalTable: RelOptTable,
        calcProgram: Option[RexProgram],
        dynamicOptionsOnTemporalTable: util.Map[String, String]): CommonPhysicalLookupJoin = {
      doTransform(join, input, temporalTable, calcProgram, dynamicOptionsOnTemporalTable)
    }
  }

  class SnapshotOnCalcTableScanRule
    extends BaseSnapshotOnCalcTableScanRule("BatchPhysicalSnapshotOnCalcTableScanRule") {

    override protected def transform(
        join: FlinkLogicalJoin,
        input: FlinkLogicalRel,
        temporalTable: RelOptTable,
        calcProgram: Option[RexProgram],
        dynamicOptionsOnTemporalTable: util.Map[String, String]): CommonPhysicalLookupJoin = {
      doTransform(join, input, temporalTable, calcProgram, dynamicOptionsOnTemporalTable)
    }

  }

  private def doTransform(
      join: FlinkLogicalJoin,
      input: FlinkLogicalRel,
      temporalTable: RelOptTable,
      calcProgram: Option[RexProgram],
      dynamicOptionsOnTemporalTable: util.Map[String, String]): BatchPhysicalLookupJoin = {
    val joinInfo = join.analyzeCondition
    val cluster = join.getCluster

    val providedTrait = join.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)
    val requiredTrait = input.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)
    val convInput = RelOptRule.convert(input, requiredTrait)
    new BatchPhysicalLookupJoin(
      cluster,
      providedTrait,
      convInput,
      temporalTable,
      calcProgram,
      joinInfo,
      join.getJoinType,
      dynamicOptionsOnTemporalTable)
  }
}
