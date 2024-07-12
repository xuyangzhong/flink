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
package org.apache.flink.table.planner.runtime.stream.sql

import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils.StreamingWithAggTestBase.AggMode
import org.apache.flink.table.planner.runtime.utils.StreamingWithMiniBatchTestBase.MiniBatchMode
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension
import org.apache.flink.types.Row

import org.junit.jupiter.api.{BeforeEach, Test, TestTemplate}
import org.junit.jupiter.api.extension.ExtendWith

@ExtendWith(Array(classOf[ParameterizedTestExtension]))
class AggSSEWindowITCase(aggMode: AggMode, miniBatch: MiniBatchMode, backend: StateBackendMode)
  extends AggSSEITCaseBase(aggMode, miniBatch, backend) {

  val data1: Seq[Row] = Seq(
    genRow("+I", 1, "M", 10, 150, "2024-03-13T10:11:20", "0"),
    genRow("+I", 2, "W", 12, 180, "2024-03-13T10:12:15", "0"),
    genRow("-U", 1, "M", 10, 150, "2024-03-13T10:11:20", "0"),
    genRow("+U", 1, "M", 10, 120, "2024-03-13T10:11:20", "0"),
    genRow("-U", 2, "W", 12, 180, "2024-03-13T10:12:15", "0"),
    genRow("+U", 2, "W", 12, 180, "2024-03-13T10:12:15", "1"),
    genRow("+I", 3, "W", 16, 100, "2024-03-13T10:13:27", "1"),
    genRow("+I", 4, "W", 18, 120, "2024-03-13T10:14:32", "1"),
    genRow("-U", 4, "W", 18, 120, "2024-03-13T10:14:32", "1"),
    genRow("+U", 4, "M", 10, 140, "2024-03-13T10:14:32", "1"),
    genRow("+I", 5, "M", 14, 120, "2024-03-13T10:15:46", "0"),
    genRow("-U", 3, "W", 16, 100, "2024-03-13T10:13:27", "1"),
    genRow("+U", 3, "W", 16, 100, "2024-03-13T10:13:28", "0"),
    genRow("+I", 6, "W", 16, 160, "2024-03-13T10:16:19", "1"),
    genRow("+I", 7, "W", 12, 140, "2024-03-13T10:17:21", "0"),
    genRow("+I", 8, "M", 18, 140, "2024-03-13T10:18:46", "0")
  )

  val data2: Seq[RowData] = Seq(
    genRowData("+I", 1, "M", 10, 150, "2024-03-13T10:11:20", "0"),
    genRowData("+I", 2, "W", 12, 180, "2024-03-13T10:12:15", "0"),
    genRowData("-U", 1, "M", 10, 150, "2024-03-13T10:11:20", "0"),
    genRowData("+U", 1, "M", 10, 120, "2024-03-13T10:11:20", "0"),
    genRowData("-U", 2, "W", 12, 180, "2024-03-13T10:12:15", "0"),
    genRowData("+U", 2, "W", 12, 180, "2024-03-13T10:12:15", "1"),
    genRowData("+I", 3, "W", 16, 100, "2024-03-13T10:13:27", "1"),
    genRowData("+I", 4, "W", 18, 120, "2024-03-13T10:14:32", "1"),
    genRowData("-U", 4, "W", 18, 120, "2024-03-13T10:14:32", "1"),
    genRowData("+U", 4, "M", 10, 140, "2024-03-13T10:14:32", "1"),
    genRowData("+I", 5, "M", 14, 120, "2024-03-13T10:15:46", "0"),
    genRowData("-U", 3, "W", 16, 100, "2024-03-13T10:13:27", "1"),
    genRowData("+U", 3, "W", 16, 100, "2024-03-13T10:13:28", "0"),
    genRowData("+I", 6, "W", 16, 160, "2024-03-13T10:16:19", "1"),
    genRowData("+I", 7, "W", 12, 140, "2024-03-13T10:17:21", "0"),
    genRowData("+I", 8, "M", 18, 140, "2024-03-13T10:18:46", "0")
  )

  @BeforeEach
  override def before(): Unit = {
    parallelism = 4
    super.before()

    // MyTable1 use org.apache.flink.streaming.api.functions.source.FromElementsFunction
    // sleep a little time after all records was emitted to make it easy to reproduce the problem
    val data1Id = TestValuesTableFactory.registerData(data1)
    tEnv.executeSql(s"""
                       |CREATE TABLE MyTable1 (
                       |  id int,
                       |  gender STRING,
                       |  age int,
                       |  height int,
                       |  create_time timestamp(3),
                       |  version string,
                       |  proc_time AS PROCTIME(),
                       |  WATERMARK FOR `create_time` AS `create_time` - INTERVAL '0' SECOND,
                       |  PRIMARY KEY (id) NOT ENFORCED
                       |) WITH (
                       | 'connector' = 'values',
                       | 'data-id' = '$data1Id',
                       | 'changelog-mode' = 'I,UA,D',
                       | 'disable-lookup' = 'true',
                       | 'bounded' = 'true'
                       |)
                       |""".stripMargin)

    // MyTable2 use org.apache.flink.table.planner.factories.TestValuesTableFactory$TestValuesScanTableSourceWithInternalData
    // sleep a little time after all records was emitted to make it easy to reproduce the problem
    val data2Id = TestValuesTableFactory.registerRowData(data2)
    tEnv.executeSql(s"""
                       |CREATE TABLE MyTable2 (
                       |  id int,
                       |  gender STRING,
                       |  age int,
                       |  height int,
                       |  create_time timestamp(3),
                       |  version string,
                       |  proc_time AS PROCTIME(),
                       |  WATERMARK FOR `create_time` AS `create_time` - INTERVAL '0' SECOND,
                       |  PRIMARY KEY (id) NOT ENFORCED
                       |) WITH (
                       | 'connector' = 'values',
                       | 'data-id' = '$data2Id',
                       | 'changelog-mode' = 'I,UA,D',
                       | 'disable-lookup' = 'true',
                       | 'source.sleep-after-elements' = '16',
                       | 'source.sleep-time' = '100ms',
                       | 'register-internal-data' = 'true',
                       | 'bounded' = 'true'
                       |)
                       |""".stripMargin)
  }

  @TestTemplate
  def testRetractHopWindowWithFromElementsFunction(): Unit = {
    // Use FromElementsFunction to reproduce the problem
    val sqlQuery =
      "select version, " +
        "HOP_START(create_time, INTERVAL '5' MINUTES, INTERVAL '10' MINUTES) AS w_start, " +
        "max(height) as max_height, " +
        "count(age) as count_age, " +
        "sum(height) as sum_height " +
        "from MyTable1 " +
        "group by HOP(create_time, INTERVAL '5' MINUTES, INTERVAL '10' MINUTES), version"
    val expected = List(
      "0,2024-03-13T10:05,120,1,120",
      "0,2024-03-13T10:10,140,5,620",
      "0,2024-03-13T10:15,140,3,400",
      "1,2024-03-13T10:05,180,3,420",
      "1,2024-03-13T10:10,180,3,480",
      "1,2024-03-13T10:15,160,1,160"
    )
    executeSql(sqlQuery, expected, List(0, 1))
  }

  @TestTemplate
  def testRetractHopWindowWithScanTableSource(): Unit = {
    // use TestValuesScanTableSourceWithInternalData to reproduce the problem
    val sqlQuery =
      "select version, " +
        "HOP_START(create_time, INTERVAL '5' MINUTES, INTERVAL '10' MINUTES) AS w_start, " +
        "max(height) as max_height, " +
        "count(age) as count_age, " +
        "sum(height) as sum_height " +
        "from MyTable2 " +
        "group by HOP(create_time, INTERVAL '5' MINUTES, INTERVAL '10' MINUTES), version"
    val expected = List(
      "0,2024-03-13T10:05,120,1,120",
      "0,2024-03-13T10:10,140,5,620",
      "0,2024-03-13T10:15,140,3,400",
      "1,2024-03-13T10:05,180,3,420",
      "1,2024-03-13T10:10,180,3,480",
      "1,2024-03-13T10:15,160,1,160"
    )
    executeSql(sqlQuery, expected, List(0, 1))
  }

  @TestTemplate
  def testRetractHopTVFWithFromElementsFunction(): Unit = {
    // Use FromElementsFunction to reproduce the problem
    val sqlQuery =
      "select version, " +
        "window_start, " +
        "max(height) as max_height, " +
        "count(age) as count_age, " +
        "sum(height) as sum_height " +
        "from Table(HOP(table MyTable1, DESCRIPTOR(create_time), INTERVAL '5' MINUTES, INTERVAL '10' MINUTES)) " +
        "group by window_start, window_end, version"
    val expected = List(
      "0,2024-03-13T10:05,120,1,120",
      "0,2024-03-13T10:10,140,5,620",
      "0,2024-03-13T10:15,140,3,400",
      "1,2024-03-13T10:05,180,3,420",
      "1,2024-03-13T10:10,180,3,480",
      "1,2024-03-13T10:15,160,1,160"
    )
    executeSql(sqlQuery, expected, List(0, 1))
  }

  @TestTemplate
  def testRetractHopTVFWithScanTableSource(): Unit = {
    // use TestValuesScanTableSourceWithInternalData to reproduce the problem
    val sqlQuery =
      "select version, " +
        "window_start, " +
        "max(height) as max_height, " +
        "count(age) as count_age, " +
        "sum(height) as sum_height " +
        "from Table(HOP(table MyTable2, DESCRIPTOR(create_time), INTERVAL '5' MINUTES, INTERVAL '10' MINUTES)) " +
        "group by window_start, window_end, version"
    val expected = List(
      "0,2024-03-13T10:05,120,1,120",
      "0,2024-03-13T10:10,140,5,620",
      "0,2024-03-13T10:15,140,3,400",
      "1,2024-03-13T10:05,180,3,420",
      "1,2024-03-13T10:10,180,3,480",
      "1,2024-03-13T10:15,160,1,160"
    )
    executeSql(sqlQuery, expected, List(0, 1))
  }
}
