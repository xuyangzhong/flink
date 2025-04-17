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

package org.apache.flink.table.planner.plan.stream.sql

import org.apache.flink.table.api.{DataTypes, ExplainDetail, Schema}
import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.catalog.{CatalogTable, ObjectPath, ResolvedCatalogTable}
import org.apache.flink.table.planner.{JHashMap, JMap}
import org.apache.flink.table.planner.utils.{TableTestBase, TestingTableEnvironment}

import org.junit.jupiter.api.{BeforeEach, Test}

import java.util.Collections

class DeltaJoinTest extends TableTestBase {

  private val util = streamTestUtil()
  private val tEnv: TestingTableEnvironment = util.tableEnv.asInstanceOf[TestingTableEnvironment]

  private val testComment = "test comment"
  private val testValuesTableOptions: JMap[String, String] = {
    val options = new JHashMap[String, String]()
    options.put("connector", "values")
    options.put("bounded", "false")
    options.put("enable-projection-push-down", "false")
    options.put("changelog-mode", "I")
    options.put("sink-insert-only", "false")
    options.put("sink-changelog-mode-enforced", "I,UA,UB,D")
    options.put("async", "true")
    options
  }

  @BeforeEach
  def setup(): Unit = {
    util.tableConfig.set(
      OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
      OptimizerConfigOptions.DeltaJoinStrategy.FORCE)

    addTable(
      "src1",
      Schema
        .newBuilder()
        .column("a0", DataTypes.INT.notNull)
        .column("a1", DataTypes.DOUBLE.notNull)
        .column("a2", DataTypes.STRING)
        .column("a3", DataTypes.INT)
        .column("a4", DataTypes.DOUBLE)
        .column("a5", DataTypes.DOUBLE)
        .primaryKey("a0", "a1")
        .index("a0", "a1")
        .build()
    )

    addTable(
      "src2",
      Schema
        .newBuilder()
        .column("b0", DataTypes.INT.notNull)
        .column("b1", DataTypes.DOUBLE.notNull)
        .column("b2", DataTypes.STRING)
        .column("b3", DataTypes.INT)
        .column("b4", DataTypes.DOUBLE)
        .column("b5", DataTypes.DOUBLE)
        .primaryKey("b0", "b1")
        .index("b1", "b0")
        .build()
    )

    addTable(
      "src3",
      Schema
        .newBuilder()
        .column("c0", DataTypes.INT.notNull)
        .column("c1", DataTypes.DOUBLE.notNull)
        .column("c2", DataTypes.STRING)
        .column("c3", DataTypes.INT)
        .column("c4", DataTypes.DOUBLE)
        .column("c5", DataTypes.DOUBLE)
        .index("c5")
        .build()
    )

    addTable(
      "non_pk_index_src",
      Schema
        .newBuilder()
        .column("d0", DataTypes.INT.notNull)
        .column("d1", DataTypes.DOUBLE.notNull)
        .column("d2", DataTypes.STRING)
        .column("d3", DataTypes.INT)
        .column("d4", DataTypes.DOUBLE)
        .column("d5", DataTypes.DOUBLE)
        .build()
    )

    addTable(
      "snk",
      Schema
        .newBuilder()
        .column("l0", DataTypes.INT.notNull)
        .column("l1", DataTypes.DOUBLE)
        .column("l2", DataTypes.STRING)
        .column("l3", DataTypes.INT)
        .column("l4", DataTypes.DOUBLE)
        .column("l5", DataTypes.DOUBLE)
        .column("r0", DataTypes.INT.notNull)
        .column("r1", DataTypes.DOUBLE)
        .column("r2", DataTypes.STRING)
        .column("r3", DataTypes.INT)
        .column("r4", DataTypes.DOUBLE)
        .column("r5", DataTypes.DOUBLE)
        .primaryKey("l0", "r0")
        .build()
    )

    addTable(
      "snk2",
      Schema
        .newBuilder()
        .column("l0", DataTypes.INT.notNull)
        .column("l1", DataTypes.DOUBLE)
        .column("l2", DataTypes.STRING)
        .column("l3", DataTypes.INT)
        .column("l4", DataTypes.DOUBLE)
        .column("l5", DataTypes.DOUBLE)
        .column("r0", DataTypes.INT.notNull)
        .column("r1", DataTypes.DOUBLE)
        .column("r2", DataTypes.STRING)
        .column("r3", DataTypes.INT)
        .column("r4", DataTypes.DOUBLE)
        .column("r5", DataTypes.DOUBLE)
        .primaryKey("l0", "r0")
        .build()
    )
  }

  @Test
  def testLookupKeysContainsPKExactlyOnBothSide(): Unit = {
    util.verifyRelPlanInsert(
      "insert into snk select * from src1 join src2 " +
        "on src1.a1 = src2.b1 " +
        "and src1.a0 = src2.b0",
      ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def test(): Unit = {
    util.addTable(s"""
                     |create table t1 (
                     |  a int primary key not enforced,
                     |  b int,
                     |  c int
                     |) with (
                     |  'connector' = 'values',
                     |  'changelog-mode' = 'I,UA',
                     |  'source.produces-delete-by-key' = 'true'
                     |)
                     |""".stripMargin)

    util.addTable(s"""
                     |create table t2 (
                     |  a int  primary key not enforced,
                     |  b int,
                     |  c int
                     |) with (
                     |  'connector' = 'values',
                     |  'sink-insert-only' = 'false',
                     |  'sink-changelog-mode-enforced' = 'I,UA,D',
                     |  'sink.supports-delete-by-key' = 'true'
                     |)
                     |""".stripMargin)

    util.verifyRelPlanInsert(
      "insert into t2 select a, b, c + 1 from t1",
      ExplainDetail.CHANGELOG_MODE)
  }

  private def addTable(
      tableName: String,
      schema: Schema,
      extraOptions: JMap[String, String] = Collections.emptyMap()): Unit = {
    val currentCatalog = tEnv.getCurrentCatalog
    val currentDatabase = tEnv.getCurrentDatabase
    val tablePath = new ObjectPath(currentDatabase, tableName)
    val catalog = tEnv.getCatalog(currentCatalog).get()
    val schemaResolver = tEnv.getCatalogManager.getSchemaResolver

    val options = new JHashMap[String, String](testValuesTableOptions)
    options.putAll(extraOptions)

    val original =
      CatalogTable.newBuilder().schema(schema).comment(testComment).options(options).build()
    val resolvedTable = new ResolvedCatalogTable(original, schemaResolver.resolve(schema))

    catalog.createTable(tablePath, resolvedTable, false)
  }
}
