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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.state.forst.ForStStateBackendFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

/** Test program. */
public abstract class TestProgram {

    protected final long dataCount;

    private final String remoteFilePath;

    public TestProgram(long dataCount, String remoteFilePath) {
        this.dataCount = dataCount;
        this.remoteFilePath = remoteFilePath;
    }

    public abstract OneInputStreamOperator<Tuple2<String, Long>, Tuple3<String, Long, Long>>
            getOperator();

    public abstract boolean isFinished();

    public void run() throws Exception {
        OneInputStreamOperator<Tuple2<String, Long>, Tuple3<String, Long, Long>> testOp =
                getOperator();
        KeyedOneInputStreamOperatorTestHarness<
                        String, Tuple2<String, Long>, Tuple3<String, Long, Long>>
                testHarness =
                        new KeyedOneInputStreamOperatorTestHarness<>(
                                testOp,
                                (KeySelector<Tuple2<String, Long>, String>) value -> value.f0,
                                BasicTypeInfo.STRING_TYPE_INFO);

        Configuration conf = new Configuration();
        conf.setString("state.backend", "org.apache.flink.state.forst.ForStStateBackendFactory");

        testHarness.setStateBackend(
                new ForStStateBackendFactory()
                        .createFromConfig(conf, this.getClass().getClassLoader()));

        testHarness.open();

        MySource source = new MySource(testHarness);
        ExecutorService sourceExecutor = Executors.newSingleThreadExecutor();
        sourceExecutor.submit(
                () -> {
                    try {
                        source.run();
                    } catch (Exception e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                });

        waitUntil(this::isFinished);
        testHarness.close();
    }

    private static void waitUntil(Supplier<Boolean> finishedCondition) {
        try {
            while (!finishedCondition.get()) {
                Thread.sleep(1);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private class MySource {

        private final String[] keys = new String[] {"a", "b", "c", "d"};

        private final Random random = new Random(1L);
        private final int randomBound = 5;
        private final KeyedOneInputStreamOperatorTestHarness<
                        String, Tuple2<String, Long>, Tuple3<String, Long, Long>>
                testHarness;

        public MySource(
                KeyedOneInputStreamOperatorTestHarness<
                                String, Tuple2<String, Long>, Tuple3<String, Long, Long>>
                        testHarness) {
            this.testHarness = testHarness;
        }

        public void run() throws Exception {
            for (long i = 0; i < dataCount; i++) {
                String k = keys[(int) (i % keys.length)];
                testHarness.processElement(Tuple2.of(k, (long) random.nextInt(randomBound)), 0L);
            }
        }
    }
}
