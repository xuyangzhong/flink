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

package org.apache.flink.table.runtime.operators.aggregate.window;

import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.aggregate.window.builder.UnslicingWindowAggOperatorBuilder;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.operators.window.windowtvf.unslicing.UnsliceAssigner;
import org.apache.flink.table.runtime.operators.window.windowtvf.unslicing.UnsliceAssigners;
import org.apache.flink.table.runtime.operators.window.windowtvf.unslicing.UnslicingWindowOperator;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.flink.table.data.TimestampData.fromEpochMillis;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for window aggregate operators created by {@link UnslicingWindowAggOperatorBuilder}. */
@RunWith(Parameterized.class)
public class UnslicingWindowAggOperatorTest extends WindowAggOperatorTestBase {

    public UnslicingWindowAggOperatorTest(ZoneId shiftTimeZone) {
        super(shiftTimeZone);
    }

    @Test
    public void testEventTimeSessionWindows() throws Exception {
        final UnsliceAssigner<TimeWindow> assigner =
                UnsliceAssigners.session(2, shiftTimeZone, Duration.ofSeconds(3));

        final UnslicingSumAndCountAggsFunction aggsFunction =
                new UnslicingSumAndCountAggsFunction();
        UnslicingWindowOperator<RowData, ?> operator =
                (UnslicingWindowOperator<RowData, ?>)
                        UnslicingWindowAggOperatorBuilder.builder()
                                .inputSerializer(INPUT_ROW_SER)
                                .shiftTimeZone(shiftTimeZone)
                                .keySerializer(KEY_SER)
                                .unsliceAssigner(assigner)
                                .aggregate(wrapGenerated(aggsFunction), ACC_SER)
                                .build();

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);

        testHarness.setup(OUT_SERIALIZER);
        testHarness.open();

        // process elements
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        // add elements out-of-order
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(3999L)));
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(3000L)));

        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(20L)));
        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(0L)));
        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(999L)));

        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(1998L)));
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(1999L)));
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(1000L)));

        testHarness.processWatermark(new Watermark(999));
        expectedOutput.add(new Watermark(999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark(new Watermark(1999));
        expectedOutput.add(new Watermark(1999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // do a snapshot, close and restore again
        testHarness.prepareSnapshotPreBarrier(0L);
        OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
        testHarness.close();

        assertThat(aggsFunction.closeCalled.get()).as("Close was not called.").isGreaterThan(0);

        expectedOutput.clear();
        testHarness = createTestHarness(operator);
        testHarness.setup();
        testHarness.initializeState(snapshot);
        testHarness.open();

        testHarness.processWatermark(new Watermark(2999));
        expectedOutput.add(new Watermark(2999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark(new Watermark(3999));
        expectedOutput.add(insertRecord("key1", 3L, 3L, localMills(0L), localMills(3999L)));
        expectedOutput.add(new Watermark(3999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // late element
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(3500L)));

        testHarness.processWatermark(new Watermark(4999));
        expectedOutput.add(new Watermark(4999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // late for all assigned windows, should be dropped
        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(999L)));

        testHarness.processWatermark(new Watermark(5999));
        expectedOutput.add(new Watermark(5999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark(new Watermark(6999));
        expectedOutput.add(insertRecord("key2", 6L, 6L, localMills(1000L), localMills(6999L)));
        expectedOutput.add(new Watermark(6999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // those don't have any effect...
        testHarness.processWatermark(new Watermark(7999));
        testHarness.processWatermark(new Watermark(8999));
        expectedOutput.add(new Watermark(7999));
        expectedOutput.add(new Watermark(8999));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        assertThat(operator.getNumLateRecordsDropped().getCount()).isEqualTo(1);

        testHarness.close();
    }

    @Test
    public void testProcessingTimeSessionWindows() throws Exception {
        final UnsliceAssigner<TimeWindow> assigner =
                UnsliceAssigners.session(-1, shiftTimeZone, Duration.ofSeconds(3));

        final UnslicingSumAndCountAggsFunction aggsFunction =
                new UnslicingSumAndCountAggsFunction();
        UnslicingWindowOperator<RowData, ?> operator =
                (UnslicingWindowOperator<RowData, ?>)
                        UnslicingWindowAggOperatorBuilder.builder()
                                .inputSerializer(INPUT_ROW_SER)
                                .shiftTimeZone(shiftTimeZone)
                                .keySerializer(KEY_SER)
                                .unsliceAssigner(assigner)
                                .aggregate(wrapGenerated(aggsFunction), ACC_SER)
                                .build();

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);

        testHarness.setup(OUT_SERIALIZER);
        testHarness.open();

        // process elements
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T00:00:03"));

        // timestamp is ignored in processing time
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(Long.MAX_VALUE)));
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(7000L)));
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(7000L)));

        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(7000L)));
        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(7000L)));

        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T00:00:06"));

        expectedOutput.add(
                insertRecord(
                        "key2",
                        3L,
                        3L,
                        epochMills(UTC_ZONE_ID, "1970-01-01T00:00:03"),
                        epochMills(UTC_ZONE_ID, "1970-01-01T00:00:06")));
        expectedOutput.add(
                insertRecord(
                        "key1",
                        2L,
                        2L,
                        epochMills(UTC_ZONE_ID, "1970-01-01T00:00:03"),
                        epochMills(UTC_ZONE_ID, "1970-01-01T00:00:06")));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T00:00:07"));
        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(7000L)));

        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T00:00:08"));
        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(7000L)));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T00:00:12"));

        expectedOutput.add(
                insertRecord(
                        "key1",
                        2L,
                        2L,
                        epochMills(UTC_ZONE_ID, "1970-01-01T00:00:07"),
                        epochMills(UTC_ZONE_ID, "1970-01-01T00:00:11")));

        assertThat(operator.getWatermarkLatency().getValue()).isEqualTo(Long.valueOf(0L));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    /** A test agg function for {@link UnslicingWindowAggOperatorTest}. */
    protected static class UnslicingSumAndCountAggsFunction
            extends SumAndCountAggsFunctionBase<TimeWindow> {

        @Override
        protected long getWindowStart(TimeWindow window) {
            return window.getStart();
        }

        @Override
        protected long getWindowEnd(TimeWindow window) {
            return window.getEnd();
        }
    }

    @Parameterized.Parameters(name = "TimeZone = {0}")
    public static Collection<Object[]> runMode() {
        return Arrays.asList(new Object[] {UTC_ZONE_ID}, new Object[] {SHANGHAI_ZONE_ID});
    }
}
