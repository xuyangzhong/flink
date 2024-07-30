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

package org.apache.flink.table.test.async;

import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.runtime.state.v2.ValueStateDescriptor;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.operators.asyncprocessing.AbstractAsyncStateStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** ThenCombineProgram. */
public class ThenCombineProgram extends TestProgram {

    private final TestOperator testOperator;

    public ThenCombineProgram(long dataCount, String remoteFilePath) {
        super(dataCount, remoteFilePath);
        this.testOperator = new TestOperator();
    }

    @Override
    public OneInputStreamOperator<Tuple2<String, Long>, Tuple3<String, Long, Long>> getOperator() {
        return testOperator;
    }

    @Override
    public boolean isFinished() {
        return testOperator.processed.get() == dataCount;
    }

    private static class TestOperator
            extends AbstractAsyncStateStreamOperator<Tuple3<String, Long, Long>>
            implements OneInputStreamOperator<Tuple2<String, Long>, Tuple3<String, Long, Long>> {

        final AtomicInteger processed = new AtomicInteger(0);

        ValueState<Long> sumState;
        ValueState<Long> countState;

        @Override
        public void open() throws Exception {
            super.open();

            sumState =
                    getRuntimeContext()
                            .getValueState(
                                    new ValueStateDescriptor<>(
                                            "sum", BasicTypeInfo.LONG_TYPE_INFO));

            countState =
                    getRuntimeContext()
                            .getValueState(
                                    new ValueStateDescriptor<>(
                                            "count", BasicTypeInfo.LONG_TYPE_INFO));
        }

        @Override
        public void processElement(StreamRecord<Tuple2<String, Long>> element) throws Exception {
            Tuple2<String, Long> record = element.getValue();
            String key = record.f0;
            long value = record.f1;

            AtomicLong count = new AtomicLong();
            AtomicLong sum = new AtomicLong();
            System.err.println(String.format("Begin: %s", record));
            countState
                    .asyncValue()
                    .thenCombine(
                            sumState.asyncValue(),
                            (preCount, preSum) -> {
                                count.set((preCount != null ? preCount : 0L) + 1L);
                                sum.set((preSum != null ? preSum : 0L) + value);
                                return null;
                            })
                    .thenAccept(
                            VOID -> {
                                if ((count.get() + sum.get()) % 2 == 0) {
                                    compute(key, "yes");
                                } else {
                                    compute(key, "no");
                                }
                            })
                    .thenAccept(
                            VOID ->
                                    StateFutureUtils.combineAll(
                                                    Arrays.asList(
                                                            sumState.asyncUpdate(sum.get()),
                                                            countState.asyncUpdate(count.get())))
                                            .thenAccept(
                                                    VOID2 -> {
                                                        System.err.println(
                                                                String.format(
                                                                        "Finish: key: %s, sum: %s, count: %s",
                                                                        key,
                                                                        sum.get(),
                                                                        count.get()));
                                                        processed.incrementAndGet();
                                                    }));
        }

        private void compute(String key, String message) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException ignore) {
            }
        }
    }

    public static void main(String[] args) throws Exception {
        ThenCombineProgram test = new ThenCombineProgram(1L, "");
        test.run();
    }
}
