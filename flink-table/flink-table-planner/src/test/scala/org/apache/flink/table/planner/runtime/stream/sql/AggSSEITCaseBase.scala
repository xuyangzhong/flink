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

import org.apache.flink.api.common.{JobID, JobStatus}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.configuration.{CheckpointingOptions, ExecutionOptions, HeartbeatManagerOptions}
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions
import org.apache.flink.streaming.api.functions.source.FromElementsFunction
import org.apache.flink.streaming.api.scala.{createTypeInformation, DataStream}
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.data.{GenericRowData, RowData, StringData, TimestampData}
import org.apache.flink.table.data.binary.{BinaryRowData, BinaryStringData}
import org.apache.flink.table.data.writer.BinaryRowWriter
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.factories.TestValuesTableFactory.{changelogRow, parseRowKind}
import org.apache.flink.table.planner.runtime.utils._
import org.apache.flink.table.planner.runtime.utils.StreamingWithAggTestBase.{AggMode, LocalGlobalOff, LocalGlobalOn}
import org.apache.flink.table.planner.runtime.utils.StreamingWithMiniBatchTestBase.{MiniBatchMode, MiniBatchOff, MiniBatchOn}
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.{HEAP_BACKEND, ROCKSDB_BACKEND, StateBackendMode}
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension
import org.apache.flink.types.{Row, RowKind}
import org.apache.flink.util.{CloseableIterator, FlinkRuntimeException}

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.api.extension.ExtendWith
import org.slf4j.{Logger, LoggerFactory}

import java.time.LocalDateTime
import java.util
import java.util.Optional

import scala.collection.{mutable, Seq}
import scala.collection.JavaConversions._

@ExtendWith(Array(classOf[ParameterizedTestExtension]))
class AggSSEITCaseBase(aggMode: AggMode, miniBatch: MiniBatchMode, backend: StateBackendMode)
  extends StreamingWithAggTestBase(aggMode, miniBatch, backend) {

  protected val log: Logger = LoggerFactory.getLogger(getClass)

  var waitTimeout: Long = 10000

  @BeforeEach
  override def before(): Unit = {
    super.before()
    env.setRestartStrategy(RestartStrategies.noRestart())
  }

  @AfterEach
  override def after(): Unit = {
    StreamTestSink.clear()
    TestValuesTableFactory.clearAllData()
  }

  def genRow(
      kind: String,
      id: Int,
      gender: String,
      age: Int,
      height: Int,
      createTime: String,
      version: String): Row = {
    changelogRow(
      kind,
      new Integer(id),
      gender,
      new Integer(age),
      new Integer(height),
      LocalDateTime.parse(createTime),
      version)
  }

  def genRowData(
      kind: String,
      id: Int,
      gender: String,
      age: Int,
      height: Int,
      createTime: String,
      version: String): RowData = {
    val rowData = new GenericRowData(6)
    rowData.setRowKind(parseRowKind(kind))
    rowData.setField(0, id)
    rowData.setField(1, new BinaryStringData(gender))
    rowData.setField(2, age)
    rowData.setField(3, height)
    rowData.setField(4, TimestampData.fromLocalDateTime(LocalDateTime.parse(createTime)))
    rowData.setField(5, new BinaryStringData(version))
    rowData
  }

  def executeSql(sqlQuery: String, expectedResults: List[String], keyIndexes: Seq[Int]): Unit = {
    // execute sql
    val tableResult = tEnv.executeSql(sqlQuery)
    log.info("SQL Plan: {}", tEnv.explainSql(sqlQuery))
    val jobId = tableResult.getJobClient.get().getJobID
    if (expectedResults == null) { // expect failed
      waitUntilFailed(jobId)
      assertThat(miniCluster.getJobStatus(jobId).get() == JobStatus.FAILED).isTrue
      miniCluster.cancelJob(jobId)
    } else { // expect running
      waitUntilFinished(jobId)
      val rowIterator = tableResult.collect()
      val executeResults = formatResult(rowIterator, keyIndexes)
      assertThat(executeResults.sorted).isEqualTo(expectedResults.sorted)
    }
  }

  def waitUntilCanceled(jobId: JobID): Unit = {
    waitUntilTargetStatus(jobId, JobStatus.CANCELED, waitTimeout)
  }

  def waitUntilFinished(jobId: JobID): Unit = {
    waitUntilTargetStatus(jobId, JobStatus.FINISHED, waitTimeout)
  }

  def waitUntilFailed(jobId: JobID): Unit = {
    waitUntilTargetStatus(jobId, JobStatus.FAILED, waitTimeout)
  }

  def waitUntilTargetStatus(jobId: JobID, targetStatus: JobStatus, timeout: Long): Unit = {
    var remainTime = timeout
    var status = miniCluster.getJobStatus(jobId).get()
    while (status != targetStatus && remainTime > 0) {
      Thread.sleep(50)
      remainTime -= 50
      status = miniCluster.getJobStatus(jobId).get()
    }
    if (status != targetStatus) {
      throw new FlinkRuntimeException(
        s"The status still has not be changed to $targetStatus after $timeout ms")
    }
  }

  def formatResult(rowIterator: CloseableIterator[Row], keyIndexes: Seq[Int]): List[String] = {
    if (keyIndexes == null || keyIndexes.isEmpty) {
      parseResult(rowIterator)
    } else {
      parseResult(rowIterator, keyIndexes)
    }
  }

  def parseResult(rowIterator: CloseableIterator[Row]): List[String] = {
    val result = new util.ArrayList[String]
    var i = 1
    while (rowIterator.hasNext) {
      val row = rowIterator.next()
      log.info("Receive record [{}]:{}", i, row)
      var rowStr = ""
      for (i <- 0 until row.getArity) {
        val value = row.getField(i)
        val str = if (value == null) "null" else value.toString
        rowStr = if (rowStr == "") str else (rowStr + "," + str)
      }
      result.add(rowStr)
      i += 1
    }
    result.toList
  }

  def parseResult(rowIterator: CloseableIterator[Row], keyIndexes: Seq[Int]): List[String] = {
    val tempMap = new util.LinkedHashMap[String, String]
    var i = 1
    while (rowIterator.hasNext) {
      val row = rowIterator.next()
      log.info("Receive data [{}]:{}", i, row)
      if (row.getKind != RowKind.DELETE && row.getKind != RowKind.UPDATE_BEFORE) {
        var key = ""
        var rowStr = ""
        for (i <- 0 until row.getArity) {
          if (keyIndexes.contains(i)) {
            val k = if (row.getField(i) == null) "null" else row.getField(i).toString
            key = key + "," + k
          }
          val value = row.getField(i)
          val str = if (value == null) "null" else value.toString
          rowStr = if (rowStr == "") str else (rowStr + "," + str)
        }
        tempMap.put(key, rowStr)
        i += 1
      }
    }
    tempMap.values().toList
  }
}
