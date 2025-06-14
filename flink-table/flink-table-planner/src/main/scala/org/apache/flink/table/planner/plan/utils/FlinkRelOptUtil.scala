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
package org.apache.flink.table.planner.plan.utils

import org.apache.flink.table.planner.JBoolean
import org.apache.flink.table.planner.analyze.PlanAdvice
import org.apache.flink.table.planner.calcite.{FlinkPlannerImpl, FlinkTypeFactory}
import org.apache.flink.table.planner.hint.FlinkHints
import org.apache.flink.table.planner.plan.`trait`.{MiniBatchInterval, MiniBatchMode}

import org.apache.calcite.config.NullCollation
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.{RelFieldCollation, RelHomogeneousShuttle, RelNode, RelShuttle}
import org.apache.calcite.rel.RelFieldCollation.{Direction, NullDirection}
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.hint.{Hintable, HintStrategyTable, RelHint}
import org.apache.calcite.rel.logical.{LogicalFilter, LogicalJoin, LogicalProject}
import org.apache.calcite.rex._
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.sql.SqlExplainLevel
import org.apache.calcite.sql.SqlKind._
import org.apache.calcite.util.Pair
import org.apache.commons.math3.util.ArithmeticUtils

import java.io.{PrintWriter, StringWriter}
import java.math.BigDecimal
import java.sql.{Date, Time, Timestamp}
import java.util
import java.util.Calendar

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

/** FlinkRelOptUtil provides utility methods for use in optimizing RelNodes. */
object FlinkRelOptUtil {

  /**
   * Converts a relational expression to a string. This is different from [[RelOptUtil]]#toString on
   * two points:
   *   1. Generated string by this method is in a tree style 2. Generated string by this method may
   *      have more information about RelNode, such as RelNode id, retractionTraits.
   *
   * @param rel
   *   the RelNode to convert
   * @param detailLevel
   *   detailLevel defines detail levels for EXPLAIN PLAN.
   * @param withIdPrefix
   *   whether including ID of RelNode as prefix
   * @param withChangelogTraits
   *   whether including changelog traits of RelNode (only applied to StreamPhysicalRel node at
   *   present)
   * @param withRowType
   *   whether including output rowType
   * @return
   *   explain plan of RelNode
   */
  def toString(
      rel: RelNode,
      detailLevel: SqlExplainLevel = SqlExplainLevel.DIGEST_ATTRIBUTES,
      withIdPrefix: Boolean = false,
      withChangelogTraits: Boolean = false,
      withRowType: Boolean = false,
      withUpsertKey: Boolean = false,
      withQueryBlockAlias: Boolean = false,
      withDuplicateChangesTrait: Boolean = false): String = {
    if (rel == null) {
      return null
    }
    val sw = new StringWriter
    val planWriter = new RelTreeWriterImpl(
      new PrintWriter(sw),
      detailLevel,
      withIdPrefix,
      withChangelogTraits,
      withRowType,
      withTreeStyle = true,
      withUpsertKey,
      withQueryHint = true,
      withQueryBlockAlias,
      withDuplicateChangesTrait = withDuplicateChangesTrait)
    rel.explain(planWriter)
    sw.toString
  }

  /**
   * Converts a sequence of relational expressions to a string. This is different from
   * [[RelOptUtil]]#toString and overloaded [[FlinkRelOptUtil]]#toString on following points:
   *   - Generated string by this method is in a tree style
   *   - Generated string by this method may have available [[PlanAdvice]]
   *
   * @param relNodes
   *   a sequence of [[RelNode]]s to convert
   * @param detailLevel
   *   detailLevel defines detail levels for EXPLAIN PLAN_ADVICE.
   * @param withChangelogTraits
   *   whether including changelog traits of RelNode (only applied to StreamPhysicalRel node at
   *   present)
   * @param withAdvice
   *   whether including plan advice of RelNode (only applied to StreamPhysicalRel node at present)
   * @return
   *   explain plan of RelNode
   */
  def toString(
      relNodes: Seq[RelNode],
      detailLevel: SqlExplainLevel,
      withChangelogTraits: Boolean,
      withAdvice: Boolean): String = {
    if (relNodes == null) {
      return null
    }
    val sw = new StringWriter
    val planWriter = new RelTreeWriterImpl(
      new PrintWriter(sw),
      detailLevel,
      withIdPrefix = false,
      withChangelogTraits,
      withRowType = false,
      withTreeStyle = true,
      withUpsertKey = false,
      withQueryHint = true,
      withQueryBlockAlias = false,
      relNodes.length,
      withAdvice = withAdvice)
    relNodes.foreach {
      rel =>
        rel.explain(planWriter)
        /*
         * Reset the print indentation because plan writer is reused.
         * This is to ensure advice is always attached at the end of the whole plan.
         */
        planWriter.continue()
    }
    sw.toString
  }

  /**
   * Gets the digest for a rel tree.
   *
   * The digest of RelNode should contain the result of RelNode#explain method, retraction traits
   * (for StreamPhysicalRel) and RelNode's row type.
   *
   * Row type is part of the digest for the rare occasion that similar expressions have different
   * types, e.g. "WITH t1 AS (SELECT CAST(a as BIGINT) AS a, SUM(b) AS b FROM x GROUP BY CAST(a as
   * BIGINT)), t2 AS (SELECT CAST(a as DOUBLE) AS a, SUM(b) AS b FROM x GROUP BY CAST(a as DOUBLE))
   * SELECT t1.*, t2.* FROM t1, t2 WHERE t1.b = t2.b"
   *
   * the physical plan is:
   * {{{
   *  HashJoin(where=[=(b, b0)], join=[a, b, a0, b0], joinType=[InnerJoin],
   *    isBroadcast=[true], build=[right])
   *  :- HashAggregate(groupBy=[a], select=[a, Final_SUM(sum$0) AS b])
   *  :  +- Exchange(distribution=[hash[a]])
   *  :     +- LocalHashAggregate(groupBy=[a], select=[a, Partial_SUM(b) AS sum$0])
   *  :        +- Calc(select=[CAST(a) AS a, b])
   *  :           +- ScanTable(table=[[builtin, default, x]], fields=[a, b, c])
   *  +- Exchange(distribution=[broadcast])
   *     +- HashAggregate(groupBy=[a], select=[a, Final_SUM(sum$0) AS b])
   *        +- Exchange(distribution=[hash[a]])
   *           +- LocalHashAggregate(groupBy=[a], select=[a, Partial_SUM(b) AS sum$0])
   *              +- Calc(select=[CAST(a) AS a, b])
   *                 +- ScanTable(table=[[builtin, default, x]], fields=[a, b, c])
   * }}}
   *
   * The sub-plan of `HashAggregate(groupBy=[a], select=[a, Final_SUM(sum$0) AS b])` are different
   * because `CAST(a) AS a` has different types, where one is BIGINT type and another is DOUBLE
   * type.
   *
   * If use the result of `RelOptUtil.toString(aggregate, SqlExplainLevel.DIGEST_ATTRIBUTES)` on
   * `HashAggregate(groupBy=[a], select=[a, Final_SUM(sum$0) AS b])` as digest, we will get
   * incorrect result. So rewrite `explain_` method of `RelWriterImpl` to add row-type to digest
   * value.
   *
   * @param rel
   *   rel node tree
   * @return
   *   The digest of given rel tree.
   */
  def getDigest(rel: RelNode): String = {
    val sw = new StringWriter
    rel.explain(
      new RelTreeWriterImpl(
        new PrintWriter(sw),
        explainLevel = SqlExplainLevel.DIGEST_ATTRIBUTES,
        // ignore id, only contains RelNode's attributes
        withIdPrefix = false,
        // add retraction traits to digest for StreamPhysicalRel node
        withChangelogTraits = true,
        // add row type to digest to avoid corner case that similar
        // expressions have different types
        withRowType = true,
        // ignore tree style, only contains RelNode's attributes
        withTreeStyle = false,
        withQueryHint = true))
    sw.toString
  }

  /**
   * Returns the null direction if not specified.
   *
   * @param direction
   *   Direction that a field is ordered in.
   * @return
   *   default null direction
   */
  def defaultNullDirection(direction: Direction): NullDirection = {
    FlinkPlannerImpl.defaultNullCollation match {
      case NullCollation.FIRST => NullDirection.FIRST
      case NullCollation.LAST => NullDirection.LAST
      case NullCollation.LOW =>
        direction match {
          case Direction.ASCENDING | Direction.STRICTLY_ASCENDING => NullDirection.FIRST
          case Direction.DESCENDING | Direction.STRICTLY_DESCENDING => NullDirection.LAST
          case _ => NullDirection.UNSPECIFIED
        }
      case NullCollation.HIGH =>
        direction match {
          case Direction.ASCENDING | Direction.STRICTLY_ASCENDING => NullDirection.LAST
          case Direction.DESCENDING | Direction.STRICTLY_DESCENDING => NullDirection.FIRST
          case _ => NullDirection.UNSPECIFIED
        }
    }
  }

  /**
   * Creates a field collation with default direction.
   *
   * @param fieldIndex
   *   0-based index of field being sorted
   * @return
   *   the field collation with default direction and given field index.
   */
  def ofRelFieldCollation(fieldIndex: Int): RelFieldCollation = {
    new RelFieldCollation(
      fieldIndex,
      FlinkPlannerImpl.defaultCollationDirection,
      defaultNullDirection(FlinkPlannerImpl.defaultCollationDirection))
  }

  /**
   * Creates a field collation.
   *
   * @param fieldIndex
   *   0-based index of field being sorted
   * @param direction
   *   Direction of sorting
   * @param nullDirection
   *   Direction of sorting of nulls
   * @return
   *   the field collation.
   */
  def ofRelFieldCollation(
      fieldIndex: Int,
      direction: RelFieldCollation.Direction,
      nullDirection: RelFieldCollation.NullDirection): RelFieldCollation = {
    new RelFieldCollation(fieldIndex, direction, nullDirection)
  }

  /**
   * Gets values of [[RexLiteral]] by its broad type.
   *
   * <p> All number value (TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, DECIMAL) will be
   * converted to BigDecimal
   *
   * @param literal
   *   input RexLiteral
   * @return
   *   value of the input RexLiteral
   */
  def getLiteralValueByBroadType(literal: RexLiteral): Comparable[_] = {
    if (literal.isNull) {
      null
    } else {
      literal.getTypeName match {
        case BOOLEAN => RexLiteral.booleanValue(literal)
        case TINYINT | SMALLINT | INTEGER | BIGINT | FLOAT | DOUBLE | DECIMAL =>
          literal.getValue3.asInstanceOf[BigDecimal]
        case VARCHAR | CHAR => literal.getValueAs(classOf[String])

        // temporal types
        case DATE =>
          new Date(literal.getValueAs(classOf[Calendar]).getTimeInMillis)
        case TIME =>
          new Time(literal.getValueAs(classOf[Calendar]).getTimeInMillis)
        case TIMESTAMP =>
          new Timestamp(literal.getValueAs(classOf[Calendar]).getTimeInMillis)
        case _ =>
          throw new IllegalArgumentException(
            s"Literal type ${literal.getTypeName} is not supported!")
      }
    }
  }

  /**
   * Partitions the [[RexNode]] in two [[RexNode]] according to a predicate. The result is a pair of
   * RexNode: the first RexNode consists of RexNode that satisfy the predicate and the second
   * RexNode consists of RexNode that don't.
   *
   * For simple condition which is not AND, OR, NOT, it is completely satisfy the predicate or not.
   *
   * For complex condition Ands, partition each operands of ANDS recursively, then merge the RexNode
   * which satisfy the predicate as the first part, merge the rest parts as the second part.
   *
   * For complex condition ORs, try to pull up common factors among ORs first, if the common factors
   * is not A ORs, then simplify the question to partition the common factors expression; else the
   * input condition is completely satisfy the predicate or not based on whether all its operands
   * satisfy the predicate or not.
   *
   * For complex condition NOT, it is completely satisfy the predicate or not based on whether its
   * operand satisfy the predicate or not.
   *
   * @param expr
   *   the expression to partition
   * @param rexBuilder
   *   rexBuilder
   * @param predicate
   *   the specified predicate on which to partition
   * @return
   *   a pair of RexNode: the first RexNode consists of RexNode that satisfy the predicate and the
   *   second RexNode consists of RexNode that don't
   */
  def partition(
      expr: RexNode,
      rexBuilder: RexBuilder,
      predicate: RexNode => JBoolean): (Option[RexNode], Option[RexNode]) = {
    val condition = pushNotToLeaf(expr, rexBuilder)
    val (left: Option[RexNode], right: Option[RexNode]) = condition.getKind match {
      case AND =>
        val (leftExprs, rightExprs) =
          partition(condition.asInstanceOf[RexCall].operands, rexBuilder, predicate)
        if (leftExprs.isEmpty) {
          (None, Option(condition))
        } else {
          val l = RexUtil.composeConjunction(rexBuilder, leftExprs.asJava, false)
          if (rightExprs.isEmpty) {
            (Option(l), None)
          } else {
            val r = RexUtil.composeConjunction(rexBuilder, rightExprs.asJava, false)
            (Option(l), Option(r))
          }
        }
      case OR =>
        val e = RexUtil.pullFactors(rexBuilder, condition)
        e.getKind match {
          case OR =>
            val (leftExprs, rightExprs) =
              partition(condition.asInstanceOf[RexCall].operands, rexBuilder, predicate)
            if (leftExprs.isEmpty || rightExprs.nonEmpty) {
              (None, Option(condition))
            } else {
              val l = RexUtil.composeDisjunction(rexBuilder, leftExprs.asJava, false)
              (Option(l), None)
            }
          case _ =>
            partition(e, rexBuilder, predicate)
        }
      case NOT =>
        val operand = condition.asInstanceOf[RexCall].operands.head
        partition(operand, rexBuilder, predicate) match {
          case (Some(_), None) => (Option(condition), None)
          case (_, _) => (None, Option(condition))
        }
      case IS_TRUE =>
        val operand = condition.asInstanceOf[RexCall].operands.head
        partition(operand, rexBuilder, predicate)
      case IS_FALSE =>
        val operand = condition.asInstanceOf[RexCall].operands.head
        val newCondition = pushNotToLeaf(operand, rexBuilder, needReverse = true)
        partition(newCondition, rexBuilder, predicate)
      case _ =>
        if (predicate(condition)) {
          (Option(condition), None)
        } else {
          (None, Option(condition))
        }
    }
    (convertRexNodeIfAlwaysTrue(left), convertRexNodeIfAlwaysTrue(right))
  }

  private def partition(
      exprs: Iterable[RexNode],
      rexBuilder: RexBuilder,
      predicate: RexNode => JBoolean): (Iterable[RexNode], Iterable[RexNode]) = {
    val leftExprs = mutable.ListBuffer[RexNode]()
    val rightExprs = mutable.ListBuffer[RexNode]()
    exprs.foreach(
      expr =>
        partition(expr, rexBuilder, predicate) match {
          case (Some(first), Some(second)) =>
            leftExprs += first
            rightExprs += second
          case (None, Some(rest)) =>
            rightExprs += rest
          case (Some(interested), None) =>
            leftExprs += interested
        })
    (leftExprs, rightExprs)
  }

  private def convertRexNodeIfAlwaysTrue(expr: Option[RexNode]): Option[RexNode] = {
    expr match {
      case Some(rex) if rex.isAlwaysTrue => None
      case _ => expr
    }
  }

  private def pushNotToLeaf(
      expr: RexNode,
      rexBuilder: RexBuilder,
      needReverse: Boolean = false): RexNode = (expr.getKind, needReverse) match {
    case (AND, true) | (OR, false) =>
      val convertedExprs = expr
        .asInstanceOf[RexCall]
        .operands
        .map(pushNotToLeaf(_, rexBuilder, needReverse))
      RexUtil.composeDisjunction(rexBuilder, convertedExprs, false)
    case (AND, false) | (OR, true) =>
      val convertedExprs = expr
        .asInstanceOf[RexCall]
        .operands
        .map(pushNotToLeaf(_, rexBuilder, needReverse))
      RexUtil.composeConjunction(rexBuilder, convertedExprs, false)
    case (NOT, _) =>
      val child = expr.asInstanceOf[RexCall].operands.head
      pushNotToLeaf(child, rexBuilder, !needReverse)
    case (_, true) if expr.isInstanceOf[RexCall] =>
      val negatedExpr = RexUtil.negate(rexBuilder, expr.asInstanceOf[RexCall])
      if (negatedExpr != null) negatedExpr else RexUtil.not(expr)
    case (_, true) => RexUtil.not(expr)
    case (_, false) => expr
  }

  /** An RexVisitor to judge whether the RexNode is related to the specified index InputRef */
  class ColumnRelatedVisitor(index: Int) extends RexVisitorImpl[JBoolean](true) {

    override def visitInputRef(inputRef: RexInputRef): JBoolean = inputRef.getIndex == index

    override def visitLiteral(literal: RexLiteral): JBoolean = true

    override def visitCall(call: RexCall): JBoolean = {
      call.operands.forall(
        operand => {
          val isRelated = operand.accept(this)
          isRelated != null && isRelated
        })
    }
  }

  /** An RexVisitor to find whether this is a call on a time indicator field. */
  class TimeIndicatorExprFinder extends RexVisitorImpl[Boolean](true) {
    override def visitInputRef(inputRef: RexInputRef): Boolean = {
      FlinkTypeFactory.isTimeIndicatorType(inputRef.getType)
    }
  }

  /**
   * Merge two MiniBatchInterval as a new one.
   *
   * The Merge Logic: MiniBatchMode: (R: rowtime, P: proctime, N: None), I: Interval Possible
   * values:
   *   - (R, I = 0): operators that require watermark (window excluded).
   *   - (R, I > 0): window / operators that require watermark with minibatch enabled.
   *   - (R, I = -1): existing window aggregate
   *   - (P, I > 0): unbounded agg with minibatch enabled.
   *   - (N, I = 0): no operator requires watermark, minibatch disabled
   * ------------------------------------------------ \| A | B | merged result
   * ------------------------------------------------ \| R, I_a == 0 | R, I_b | R, gcd(I_a, I_b)
   * ------------------------------------------------ \| R, I_a == 0 | P, I_b | R, I_b
   * ------------------------------------------------ \| R, I_a > 0 | R, I_b | R, gcd(I_a, I_b)
   * ------------------------------------------------ \| R, I_a > 0 | P, I_b | R, I_a
   * ------------------------------------------------ \| R, I_a = -1 | R, I_b | R, I_a
   * ------------------------------------------------ \| R, I_a = -1 | P, I_b | R, I_a
   * ------------------------------------------------ \| P, I_a | R, I_b == 0 | R, I_a
   * ------------------------------------------------ \| P, I_a | R, I_b > 0 | R, I_b
   * ------------------------------------------------ \| P, I_a | P, I_b > 0 | P, I_a
   * ------------------------------------------------
   */
  def mergeMiniBatchInterval(
      interval1: MiniBatchInterval,
      interval2: MiniBatchInterval): MiniBatchInterval = {
    if (
      interval1 == MiniBatchInterval.NO_MINIBATCH ||
      interval2 == MiniBatchInterval.NO_MINIBATCH
    ) {
      return MiniBatchInterval.NO_MINIBATCH
    }
    interval1.getMode match {
      case MiniBatchMode.None => interval2
      case MiniBatchMode.RowTime =>
        interval2.getMode match {
          case MiniBatchMode.None => interval1
          case MiniBatchMode.RowTime =>
            val gcd = ArithmeticUtils.gcd(interval1.getInterval, interval2.getInterval)
            new MiniBatchInterval(gcd, MiniBatchMode.RowTime)
          case MiniBatchMode.ProcTime =>
            if (interval1.getInterval == 0) {
              new MiniBatchInterval(interval2.getInterval, MiniBatchMode.RowTime)
            } else {
              interval1
            }
        }
      case MiniBatchMode.ProcTime =>
        interval2.getMode match {
          case MiniBatchMode.None | MiniBatchMode.ProcTime => interval1
          case MiniBatchMode.RowTime =>
            if (interval2.getInterval > 0) {
              interval2
            } else {
              new MiniBatchInterval(interval1.getInterval, MiniBatchMode.RowTime)
            }
        }
    }
  }

  // ----- The following is mainly copied from RelOptUtil -----
  // ----- Copied Line: 537 ~ 743 -----
  // ----- Modified Line: 642 ~ 662 -----
  /**
   * Propagates the relational expression hints from root node to leaf node.
   *
   * @param rel
   *   The relational expression
   * @param reset
   *   Flag saying if to reset the existing hints before the propagation
   * @return
   *   New relational expression with hints propagated
   */
  def propagateRelHints(rel: RelNode, reset: Boolean): RelNode = {
    val node = if (reset) {
      rel.accept(new ResetHintsShuttle)
    } else {
      rel
    }
    val shuttle = new RelHintPropagateShuttle(node.getCluster.getHintStrategies)
    node.accept(shuttle)
  }

  /**
   * A [[RelShuttle]] which resets all the hints of a relational expression to what they are
   * originally like.
   *
   * <p>This would trigger a reverse transformation of what [[RelHintPropagateShuttle]] does.
   *
   * <p>Transformation rules:
   *
   * <ul> <li>Project: remove the hints that have non-empty inherit path (which means the hint was
   * not originally declared from it); <li>Aggregate: remove the hints that have non-empty inherit
   * path; <li>Join: remove all the hints; <li>TableScan: remove the hints that have non-empty
   * inherit path. </ul>
   */
  private class ResetHintsShuttle extends RelHomogeneousShuttle {
    override def visit(node: RelNode): RelNode = {
      var finalNode = visitChildren(node)
      if (node.isInstanceOf[Hintable]) {
        finalNode = ResetHintsShuttle.resetHints(finalNode.asInstanceOf[Hintable])
      }
      finalNode
    }
  }

  private object ResetHintsShuttle {
    private def resetHints(hintable: Hintable): RelNode = if (hintable.getHints.size > 0) {
      val resetHints: util.List[RelHint] = hintable.getHints
        .filter((hint: RelHint) => hint.inheritPath.size == 0)
        .toList
      hintable.withHints(resetHints)
    } else {
      hintable.asInstanceOf[RelNode]
    }
  }

  /**
   * A [[RelShuttle]] which propagates all the hints of relational expression to their children
   * nodes.
   *
   * <p>Given a plan:
   *
   * {{{
   *            Filter (Hint1)
   *                |
   *               Join
   *              /    \
   *            Scan  Project (Hint2)
   *                     |
   *                    Scan2
   * }}}
   *
   * <p>Every hint has a [[inheritPath]] (integers list) which records its propagate path, number
   * `0` represents the hint is propagated from the first(left) child, number `1` represents the
   * hint is propagated from the second(right) child, so the plan would have hints path as follows
   * (assumes each hint can be propagated to all child nodes):
   *
   * <ul> <li>Filter would have hints {Hint1[]}</li> <li>Join would have hints {Hint1[0]}</li>
   * <li>Scan would have hints {Hint1[0, 0]}</li> <li>Project would have hints {Hint1[0,1],
   * Hint2[]}</li> <li>Scan2 would have hints {[Hint1[0, 1, 0], Hint2[0]}</li> </ul>
   */
  private class RelHintPropagateShuttle private[plan] (
      /** The hint strategies to decide if a hint should be attached to a relational expression. */
      val hintStrategies: HintStrategyTable)
    extends RelHomogeneousShuttle {

    /** Stack recording the hints and its current inheritPath. */
    final private val inheritPaths =
      new util.ArrayDeque[Pair[util.List[RelHint], util.Deque[Integer]]]

    /** Visits a particular child of a parent. */
    override protected def visitChild(parent: RelNode, i: Int, child: RelNode): RelNode = {
      inheritPaths.forEach(
        (inheritPath: Pair[util.List[RelHint], util.Deque[Integer]]) => inheritPath.right.push(i))
      try {
        val child2 = child.accept(this)
        if (child2 ne child) {
          val newInputs = new util.ArrayList[RelNode](parent.getInputs)
          newInputs.set(i, child2)
          return parent.copy(parent.getTraitSet, newInputs)
        }
        parent
      } finally
        inheritPaths.forEach(
          (inheritPath: Pair[util.List[RelHint], util.Deque[Integer]]) => inheritPath.right.pop)
    }

    // FLINK MODIFICATION BEGIN
    // let hints propagating in sub query

    override def visit(other: RelNode): RelNode = {
      val node = tryToPropagateHintsInSubQuery(other)
      if (node.isInstanceOf[Hintable]) {
        visitHintable(node)
      } else {
        visitChildren(node)
      }
    }

    private def tryToPropagateHintsInSubQuery(node: RelNode): RelNode = {
      if (containsSubQuery(node)) {
        FlinkHints.resolveSubQuery(node, relNode => relNode.accept(this))
      } else {
        node
      }
    }

    // FLINK MODIFICATION END

    /**
     * Handle the [[Hintable]]s.
     *
     * <p>There are two cases to handle hints:
     *
     * <ul> <li>For TableScan: table scan is always a leaf node, attach the hints of the propagation
     * path directly;</li> <li>For other [[Hintable]]s: if the node has hints itself, that means,
     * these hints are query hints that need to propagate to its children, so we do these things:
     * <ol> <li>push the hints with empty inheritPath to the stack</li> <li>visit the children nodes
     * and propagate the hints</li> <li>pop the hints pushed in step1</li> <li>attach the hints of
     * the propagation path</li> </ol> if the node does not have hints, attach the hints of the
     * propagation path directly. </li> </ul>
     *
     * @param node
     *   [[Hintable]] to handle
     * @return
     *   New copy of the [[Hintable]] with propagated hints attached
     */
    private def visitHintable(node: RelNode) = {
      val topHints = node.asInstanceOf[Hintable].getHints
      val hasHints = topHints != null && topHints.size > 0
      val hasQueryHints = hasHints && !node.isInstanceOf[TableScan]
      if (hasQueryHints) inheritPaths.push(Pair.of(topHints, new util.ArrayDeque[Integer]))
      val node1 = visitChildren(node)
      if (hasQueryHints) inheritPaths.pop
      attachHints(node1)
    }

    private def attachHints(original: RelNode): RelNode = {
      assert(original.isInstanceOf[Hintable])
      if (inheritPaths.size > 0) {
        val hints = inheritPaths.toList
          .sorted(
            (
                o1: Pair[util.List[RelHint], util.Deque[Integer]],
                o2: Pair[util.List[RelHint], util.Deque[Integer]]) => {
              Integer.compare(o1.right.size, o2.right.size)
            })
          .map(
            (path: Pair[util.List[RelHint], util.Deque[Integer]]) =>
              RelHintPropagateShuttle
                .copyWithInheritPath(path.left, path.right))
          .foldLeft(new util.ArrayList[RelHint]())(
            (acc, hints1) => {
              acc.addAll(hints1)
              acc
            })
        val filteredHints = hintStrategies.apply(hints, original)
        if (filteredHints.size > 0) {
          return original.asInstanceOf[Hintable].attachHints(filteredHints)
        }
      }
      original
    }
  }

  private object RelHintPropagateShuttle {
    private def copyWithInheritPath(
        hints: util.List[RelHint],
        inheritPath: util.Deque[Integer]): util.List[RelHint] = {
      // Copy the Dequeue in reverse order.
      val path = new util.ArrayList[Integer]
      val iterator = inheritPath.descendingIterator
      while (iterator.hasNext) {
        path.add(iterator.next)
      }
      hints.map((hint: RelHint) => hint.copy(path)).toList
    }
  }

  // ----- Copied from RelOptUtil end -----

  /** Check if the node contains sub query. */
  def containsSubQuery(node: RelNode): Boolean = node match {
    // the all types of nodes that contain sub query can be found in
    // RexUtil.SubQueryFinder#containsSubQuery
    case project: LogicalProject =>
      RexUtil.SubQueryFinder.containsSubQuery(project)
    case filter: LogicalFilter =>
      RexUtil.SubQueryFinder.containsSubQuery(filter)
    case join: LogicalJoin =>
      RexUtil.SubQueryFinder.containsSubQuery(join)
    case _ => false
  }
}
