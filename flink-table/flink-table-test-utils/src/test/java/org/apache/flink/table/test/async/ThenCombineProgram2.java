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

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.runtime.state.v2.ValueStateDescriptor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** ThenCombineProgram. */
public class ThenCombineProgram2 extends TestProgram2 {

    private final TestFunction testFunction;

    public ThenCombineProgram2(
            long dataCount, String remoteFilePath, StreamExecutionEnvironment env) {
        super(dataCount, remoteFilePath, env);
        TestFunction.processed.set(0);
        this.testFunction = new TestFunction();
    }

    @Override
    public KeyedProcessFunction<String, Tuple2<String, Long>, Tuple3<String, Long, Long>>
            getProcessFunction() {
        return testFunction;
    }

    @Override
    public boolean isFinished() {
        return TestFunction.processed.get() == dataCount;
    }

    private static class TestFunction
            extends KeyedProcessFunction<String, Tuple2<String, Long>, Tuple3<String, Long, Long>> {

        private static volatile AtomicInteger processed = new AtomicInteger(0);

        ValueState<Long> sumState;
        ValueState<Long> countState;

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);

            sumState =
                    ((StreamingRuntimeContext) getRuntimeContext())
                            .getValueState(
                                    new ValueStateDescriptor<>(
                                            "sum", BasicTypeInfo.LONG_TYPE_INFO));

            countState =
                    ((StreamingRuntimeContext) getRuntimeContext())
                            .getValueState(
                                    new ValueStateDescriptor<>(
                                            "count", BasicTypeInfo.LONG_TYPE_INFO));
        }

        @Override
        public void processElement(
                Tuple2<String, Long> record,
                KeyedProcessFunction<String, Tuple2<String, Long>, Tuple3<String, Long, Long>>
                                .Context
                        ctx,
                Collector<Tuple3<String, Long, Long>> out)
                throws Exception {
            String key = record.f0;
            long value = record.f1;

            AtomicLong count = new AtomicLong();
            AtomicLong sum = new AtomicLong();
            //            System.err.println(String.format("Begin: %s", record));
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
                                                        //
                                                        //              System.err.println(
                                                        //
                                                        //                      String.format(
                                                        //
                                                        //                              "Finish:
                                                        // key: %s, sum: %s, count: %s",
                                                        //
                                                        //                              key,
                                                        //
                                                        //                              sum.get(),
                                                        //
                                                        //
                                                        // count.get()));
                                                        processed.incrementAndGet();
                                                    }));
        }

        private void compute(String key, String message) {
            //            try {
            //                Thread.sleep(1);
            //            } catch (InterruptedException ignore) {
            //            }
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration conf = (Configuration) env.getConfiguration();
        conf.setString("state.backend", "org.apache.flink.state.forst.ForStStateBackendFactory");
        String ossObjectName = "";
        if (!StringUtils.isNullOrWhitespaceOnly(ossObjectName)) {
            conf.setString(
                    "state.backend.forst.remote-dir",
                    String.format("oss://%s/%s/", System.getenv("OSS_BUCKET"), ossObjectName));
            conf.setString("fs.oss.endpoint", System.getenv("OSS_HOST"));
            conf.setString("fs.oss.accessKeyId", System.getenv("OSS_AK"));
            conf.setString("fs.oss.accessKeySecret", System.getenv("OSS_SK"));
        }
        env.disableOperatorChaining();
        env.setParallelism(1);

        ThenCombineProgram2 test = new ThenCombineProgram2(1L, "", env);
        test.run();
    }
}
