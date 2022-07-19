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

import org.apache.flink.annotation.Experimental
import org.apache.flink.configuration.ConfigOption
import org.apache.flink.configuration.ConfigOptions.key
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.planner.{JArrayList, JDouble, JList}
import org.apache.flink.table.planner.hint.JoinStrategy
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalLocalHashAggregate
import org.apache.flink.table.planner.plan.utils.{FlinkRelMdUtil, OperatorType}
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig
import org.apache.flink.table.planner.utils.TableConfigUtils.isOperatorDisabled

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Join, JoinRelType}
import org.apache.calcite.util.ImmutableBitSet

import java.lang.{Boolean => JBoolean, Double => JDouble}

import scala.collection.JavaConversions._

trait BatchPhysicalJoinRuleBase {

  protected def checkMatchJoinStrategy(
      call: RelOptRuleCall,
      joinStrategy: JoinStrategy): Boolean = {
    val join: Join = call.rel(0)
    val tableConfig = unwrapTableConfig(call)
    val validJoinHints = collectValidJoinHints(join, tableConfig)
    if (!validJoinHints.isEmpty) {
      validJoinHints.head == joinStrategy
    } else {
      // treat as non-join-hints
      checkJoinStrategyValid(join, tableConfig, joinStrategy, withHint = false)._1
    }
  }

  def addLocalDistinctAgg(node: RelNode, distinctKeys: Seq[Int]): RelNode = {
    val localRequiredTraitSet = node.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)
    val newInput = RelOptRule.convert(node, localRequiredTraitSet)
    val providedTraitSet = localRequiredTraitSet

    new BatchPhysicalLocalHashAggregate(
      node.getCluster,
      providedTraitSet,
      newInput,
      node.getRowType, // output row type
      node.getRowType, // input row type
      distinctKeys.toArray,
      Array.empty,
      Seq())
  }

  def chooseSemiBuildDistinct(buildRel: RelNode, distinctKeys: Seq[Int]): Boolean = {
    val tableConfig = unwrapTableConfig(buildRel)
    val mq = buildRel.getCluster.getMetadataQuery
    val ratioConf =
      tableConfig.get(BatchPhysicalJoinRuleBase.TABLE_OPTIMIZER_SEMI_JOIN_BUILD_DISTINCT_NDV_RATIO)
    val inputRows = mq.getRowCount(buildRel)
    val ndvOfGroupKey = mq.getDistinctRowCount(buildRel, ImmutableBitSet.of(distinctKeys: _*), null)
    if (ndvOfGroupKey == null) {
      false
    } else {
      ndvOfGroupKey / inputRows < ratioConf
    }
  }

  private[flink] def binaryRowRelNodeSize(relNode: RelNode): JDouble = {
    val mq = relNode.getCluster.getMetadataQuery
    val rowCount = mq.getRowCount(relNode)
    if (rowCount == null) {
      null
    } else {
      rowCount * FlinkRelMdUtil.binaryRowAverageSize(relNode)
    }
  }

  protected def collectValidJoinHints(join: Join, tableConfig: TableConfig): JList[JoinStrategy] = {
    val allHints = join.getHints
    val validHints = new JArrayList[JoinStrategy]

    allHints.forEach(
      relHint => {
        val joinHint = JoinStrategy.getJoinStrategy(relHint.hintName)
        if (checkJoinStrategyValid(join, tableConfig, joinHint, withHint = true)._1) {
          validHints.add(joinHint)
        }
      })

    validHints
  }

  def checkJoinStrategyValid(
      join: Join,
      tableConfig: TableConfig,
      triedJoinStrategy: JoinStrategy,
      withHint: Boolean): (Boolean, Boolean) = {

    triedJoinStrategy match {
      case JoinStrategy.BROADCAST =>
        checkBroadcast(join, tableConfig, withHint)

      case JoinStrategy.SHUFFLE_HASH =>
        checkShuffleHash(join, tableConfig, withHint)

      case JoinStrategy.SHUFFLE_MERGE =>
        // for SortMergeJoin, there is no diff between with hint or without hint
        // the second arg should be ignored
        (checkSortMergeJoin(join, tableConfig), false)

      case JoinStrategy.NEST_LOOP =>
        checkNestLoopJoin(join, tableConfig, withHint)
    }
  }

  private def isEquivJoin(join: Join): Boolean = {
    val joinInfo = join.analyzeCondition
    !joinInfo.pairs().isEmpty
  }

  /**
   * Decides whether the join can convert to BroadcastHashJoin.
   *
   * @param join
   *   the join node
   * @return
   *   an Tuple2 instance. The first element of tuple is true if join can convert to broadcast hash
   *   join, false else. The second element of tuple is true if left side used as broadcast side,
   *   false else.
   */
  protected def checkBroadcast(
      join: Join,
      tableConfig: TableConfig,
      withHint: Boolean): (Boolean, Boolean) = {
    if (isEquivJoin(join) || isOperatorDisabled(tableConfig, OperatorType.BroadcastHashJoin)) {
      return (false, false)
    }

    val leftSize = binaryRowRelNodeSize(join.getLeft)
    val rightSize = binaryRowRelNodeSize(join.getRight)

    if (!withHint) {
      // if leftSize or rightSize is unknown, cannot use broadcast
      if (leftSize == null || rightSize == null) {
        return (false, false)
      }

      val threshold =
        tableConfig.get(OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD)

      val rightSizeSmallerThanThreshold = rightSize <= threshold
      val leftSizeSmallerThanThreshold = leftSize <= threshold
      val leftSmallerThanRight = leftSize <= rightSize

      join.getJoinType match {
        case JoinRelType.LEFT => (rightSizeSmallerThanThreshold, false)
        case JoinRelType.RIGHT => (leftSizeSmallerThanThreshold, true)
        case JoinRelType.FULL => (false, false)
        case JoinRelType.INNER =>
          (
            leftSizeSmallerThanThreshold
              || rightSizeSmallerThanThreshold,
            leftSmallerThanRight)
        // left side cannot be used as build side in SEMI/ANTI join.
        case JoinRelType.SEMI | JoinRelType.ANTI =>
          (rightSizeSmallerThanThreshold, false)
      }
    } else {
      val isLeftToBroadcastInHint =
        getFirstArgInJoinHint(join, JoinStrategy.BROADCAST.getJoinHintName)
          .equals(JoinStrategy.LEFT_INPUT)

      join.getJoinType match {
        // if left join, must broadcast right side
        case JoinRelType.LEFT => (!isLeftToBroadcastInHint, false)
        // if left join, must broadcast left side
        case JoinRelType.RIGHT => (isLeftToBroadcastInHint, true)
        case JoinRelType.FULL => (false, false)
        case JoinRelType.INNER =>
          (true, isLeftToBroadcastInHint)
        // if left join, must broadcast right side
        case JoinRelType.SEMI | JoinRelType.ANTI =>
          (!isLeftToBroadcastInHint, false)
      }
    }
  }

  protected def checkShuffleHash(
      join: Join,
      tableConfig: TableConfig,
      withHint: Boolean): (Boolean, Boolean) = {
    if (isEquivJoin(join) || isOperatorDisabled(tableConfig, OperatorType.ShuffleHashJoin)) {
      return (false, false)
    }
    if (!withHint) {
      val leftSize = binaryRowRelNodeSize(join.getLeft)
      val rightSize = binaryRowRelNodeSize(join.getRight)
      (true, leftSize < rightSize)
    } else {
      val isLeftToBuild = getFirstArgInJoinHint(join, JoinStrategy.SHUFFLE_HASH.getJoinHintName)
        .equals(JoinStrategy.LEFT_INPUT)
      (true, isLeftToBuild)
    }
  }

  protected def checkSortMergeJoin(join: Join, tableConfig: TableConfig): Boolean = {
    if (isEquivJoin(join) || isOperatorDisabled(tableConfig, OperatorType.ShuffleHashJoin)) {
      false
    } else {
      true
    }
  }

  protected def checkNestLoopJoin(
      join: Join,
      tableConfig: TableConfig,
      withHint: Boolean): (Boolean, Boolean) = {

    if (isOperatorDisabled(tableConfig, OperatorType.NestedLoopJoin)) {
      return (false, false)
    }

    val isLeftToBuild = if (!withHint) {
      join.getJoinType match {
        case JoinRelType.LEFT => false
        case JoinRelType.RIGHT => true
        case JoinRelType.INNER | JoinRelType.FULL =>
          val leftSize = binaryRowRelNodeSize(join.getLeft)
          val rightSize = binaryRowRelNodeSize(join.getRight)
          // use left as build size if leftSize or rightSize is unknown.
          if (leftSize == null || rightSize == null) {
            true
          } else {
            leftSize <= rightSize
          }
        case JoinRelType.SEMI | JoinRelType.ANTI => false
      }

    } else {
      getFirstArgInJoinHint(join, JoinStrategy.SHUFFLE_HASH.getJoinHintName)
        .equals(JoinStrategy.LEFT_INPUT)
    }

    // all join can use NEST LOOP JOIN
    (true, isLeftToBuild)

  }

  private def getFirstArgInJoinHint(join: Join, joinHintName: String): String = {
    join.getHints.forEach(
      hint => {
        if (hint.hintName.equals(joinHintName)) {
          return hint.listOptions.get(0)
        }
      })

    // can not happen
    throw new TableException(
      String.format(
        "Fail to find the join hint `%s` among `%s`",
        joinHintName,
        java.util.Arrays.toString(join.getHints.stream().map(hint => hint.hintName).toArray)))
  }
}
object BatchPhysicalJoinRuleBase {

  // It is a experimental config, will may be removed later.
  @Experimental
  val TABLE_OPTIMIZER_SEMI_JOIN_BUILD_DISTINCT_NDV_RATIO: ConfigOption[JDouble] =
    key("table.optimizer.semi-anti-join.build-distinct.ndv-ratio")
      .doubleType()
      .defaultValue(JDouble.valueOf(0.8))
      .withDescription(
        "In order to reduce the amount of data on semi/anti join's" +
          " build side, we will add distinct node before semi/anti join when" +
          "  the semi-side or semi/anti join can distinct a lot of data in advance." +
          " We add this configuration to help the optimizer to decide whether to" +
          " add the distinct.")

  // It is a experimental config, will may be removed later.
  @Experimental
  val TABLE_OPTIMIZER_SHUFFLE_BY_PARTIAL_KEY_ENABLED: ConfigOption[JBoolean] =
    key("table.optimizer.shuffle-by-partial-key-enabled")
      .booleanType()
      .defaultValue(JBoolean.valueOf(false))
      .withDescription(
        "Enables shuffling by partial partition keys. " +
          "For example, A join with join condition: L.c1 = R.c1 and L.c2 = R.c2. " +
          "If this flag is enabled, there are 3 shuffle strategy:\n " +
          "1. L and R shuffle by c1 \n 2. L and R shuffle by c2\n " +
          "3. L and R shuffle by c1 and c2\n It can reduce some shuffle cost someTimes.")
}
