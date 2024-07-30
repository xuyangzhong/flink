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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/** Test program. */
public abstract class TestProgram2 {

    protected final long dataCount;

    private final String ossObjectName;

    private final StreamExecutionEnvironment env;

    public TestProgram2(long dataCount, String ossObjectName, StreamExecutionEnvironment env) {
        this.dataCount = dataCount;
        this.ossObjectName = ossObjectName;
        this.env = env;
    }

    public abstract boolean isFinished();

    public abstract KeyedProcessFunction<String, Tuple2<String, Long>, Tuple3<String, Long, Long>>
            getProcessFunction();

    public void run() throws Exception {
        DataStreamSource<Tuple2<String, Long>> source =
                env.addSource(new MySourceFunction(dataCount));

        source.keyBy(value -> value.f0).asyncProcess(getProcessFunction()).print();
        env.execute();

        if (!isFinished()) {
            System.err.println("Failed to finished");
        }
    }

    private static class MySourceFunction implements SourceFunction<Tuple2<String, Long>> {

        private final String[] keys;

        private final Random random;
        private final int randomBound = 5;
        private final long dataCount;

        private volatile boolean running = true;

        public MySourceFunction(long dataCount) {
            this.dataCount = dataCount;
            this.keys = new String[] {"a", "b", "c", "d"};
            this.random = new Random(1L);
        }

        @Override
        public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
            for (long i = 0; i < dataCount && running; i++) {
                String k = keys[(int) (i % keys.length)];
                ctx.collect(new Tuple2<>(k, (long) random.nextInt(randomBound)));
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
