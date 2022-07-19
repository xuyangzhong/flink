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

package org.apache.flink.table.planner.plan.optimize

import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.planner.JHashSet
import org.apache.flink.table.planner.hint.{FlinkHints, JoinStrategy}
import org.apache.flink.table.planner.plan.nodes.calcite.WatermarkAssigner

import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.{BiRel, RelNode, RelShuttleImpl}
import org.apache.calcite.rel.core.{Snapshot, TableScan}
import org.apache.calcite.rel.hint.{Hintable, RelHint}
import org.apache.calcite.rel.logical.{LogicalCorrelate, LogicalFilter, LogicalJoin, LogicalProject}
import org.apache.calcite.util.Util

import java.util.{Collections, Optional}

import scala.collection.JavaConversions._

/** Resolve and validate Hints, currently only join hints are supported. */
class HintResolver {

  private val allHints = new JHashSet[RelHint]
  private val validHints = new JHashSet[RelHint]

  /** Transforms a relational expression into another relational expression. */
  def resolve(roots: Seq[RelNode]): Seq[RelNode] = {
    val resolvedRoots = roots.map(node => node.accept(new JoinHintResolver))
    // check if there exits invalid hints
    validateHints()
    resolvedRoots
  }

  class JoinHintResolver extends RelShuttleImpl {

    override def visit(correlate: LogicalCorrelate): RelNode = {
      visitBiRel(correlate)
    }

    override def visit(join: LogicalJoin): RelNode = {
      visitBiRel(join)
    }

    private def visitBiRel(biRel: BiRel): RelNode = {
      val leftName = extractAliasOrTableName(biRel.getLeft)
      val rightName = extractAliasOrTableName(biRel.getRight)

      val existentJoinHintNames = new JHashSet[String]
      val existentKVHints = new JHashSet[RelHint]

      val newHints = biRel
        .asInstanceOf[Hintable]
        .getHints
        .flatMap(
          h =>
            if (JoinStrategy.isJoinStrategy(h.hintName)) {
              allHints.add(trimInheritPath(h))
              // if the hint
              val newOptions = h.listOptions
                .map(
                  option => {
                    if (
                      leftName.isDefined
                      && rightName.isDefined
                      && matchIdentifier(option, leftName.get)
                      && matchIdentifier(option, rightName.get)
                    ) {
                      throw new ValidationException(
                        String.format(
                          "Ambitious option: %s in hint: %s, the input " +
                            "relations are: %s, %s",
                          option,
                          h,
                          leftName,
                          rightName))
                    } else if (leftName.isDefined && matchIdentifier(option, leftName.get)) {
                      JoinStrategy.LEFT_INPUT
                    } else if (rightName.isDefined && matchIdentifier(option, rightName.get)) {
                      JoinStrategy.RIGHT_INPUT
                    } else {
                      ""
                    }
                  })
                .filter(p => p.nonEmpty)
              // check whether the join hints options are valid
              val isValidOption = JoinStrategy.validOptions(h.hintName, newOptions)
              if (isValidOption) {
                if (existentJoinHintNames.contains(h.hintName)) {
                  // if there are more than one join hints with same names,
                  // only retains the first one. written order first
                  List()
                } else {
                  existentJoinHintNames.add(h.hintName)
                  validHints.add(trimInheritPath(h))
                  List(
                    RelHint
                      .builder(h.hintName)
                      // if the hint defines more than one args, only retain the first one
                      .hintOptions(Collections.singletonList(newOptions.head))
                      .build())
                }
              } else {
                // invalid hint
                List()
              }
            } else {
              if (existentKVHints.contains(h)) {
                List()
              } else {
                existentKVHints.add(h)
                List(h)
              }
            })
        .filter(p => { !(p.kvOptions.isEmpty && p.listOptions.isEmpty) })
      val newNode = super.visitChildren(biRel)
      if (biRel.asInstanceOf[Hintable].getHints.nonEmpty) {
        val deduplicateHints = new JHashSet(newHints)
        newNode.asInstanceOf[Hintable].withHints(deduplicateHints.toList)
      } else {
        // has no hints, return original node directly.
        newNode
      }
    }

    private def trimInheritPath(hint: RelHint): RelHint = {
      val builder = RelHint.builder(hint.hintName)
      if (hint.listOptions.nonEmpty) {
        builder.hintOptions(hint.listOptions).build()
      } else {
        builder.hintOptions(hint.kvOptions).build()
      }
    }
  }

  private def validateHints(): Unit = {
    val invalidHints = allHints.diff(validHints)
    if (invalidHints.nonEmpty) {
      val msg = invalidHints.foldLeft("")(
        (msg, hint) =>
          msg + "\n`" + hint.hintName
            + "(" + hint.listOptions.mkString(", ") + ")`")
      throw new ValidationException(
        String.format(
          "The options of following hints cannot match the name of " +
            "input tables or views: %s",
          msg))
    }
  }

  /**
   * Currently, only two kinds of options are supported for join hints:
   *   1. table name (including table name under a `Snapshot`). e.g., > SELECT /*+ shuffle_hash(dim)
   *      */ * > FROM v1 JOIN dim FOR SYSTEM_TIME AS OF PROCTIME() ON v1.a = dim.a; 2. view name,
   *      e.g., > CREATE VIEW v1 as SELECT * FROM SRC; > SELECT /*+ skew(v1) */ * FROM v1 JOIN dim
   *      FOR SYSTEM_TIME AS OF PROCTIME() ON v1.a = dim.a; Note: hint option on alias is not
   *      supported now (VVR-32686124).
   */
  def extractAliasOrTableName(node: RelNode): Option[String] = {
    // check whether the input relation is converted from a view
    val aliasName = FlinkHints.getTableAlias(node)
    if (aliasName.isPresent) {
      return Some(aliasName.get())
    }
    // otherwise, the option should be a table name
    val tableScan = getTableScan(node)
    val tableName = tableScan match {
      case ts: TableScan => FlinkHints.getTableName(ts.getTable)
      case None => Optional.empty()
    }
    if (tableName.isPresent) {
      return Some(tableName.get())
    }

    None
  }

  @scala.annotation.tailrec
  private def getTableScan(node: RelNode): Option[TableScan] = {
    node match {
      case tableScan: TableScan => Some(tableScan)
      case _: LogicalProject // computed column on lookup table
          | _: WatermarkAssigner // watermark assigner on lookup table
          | _: LogicalFilter // temporal join correlating on time column of left table
          | _: Snapshot =>
        getTableScan(trimHep(node.getInput(0)))
      case _ => None
    }
  }

  private def trimHep(node: RelNode): RelNode = {
    node match {
      case hepRelVertex: HepRelVertex =>
        hepRelVertex.getCurrentRel
      case subset: RelSubset =>
        Util.first(subset.getBest, subset.getOriginal)
      case _ => node
    }
  }

  /**
   * Check whether the given hint option matches the table qualified names. For convenience, we
   * follow a simple rule: the matching is successful if the option is the suffix of the table
   * qualified names.
   */
  private def matchIdentifier(option: String, tableIdentifier: String): Boolean =
    tableIdentifier.toLowerCase.endsWith(option.toLowerCase)
}
