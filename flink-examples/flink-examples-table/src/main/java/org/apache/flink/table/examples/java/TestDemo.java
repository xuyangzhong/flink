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

package org.apache.flink.table.examples.java;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.OutputTag;

/** asd. */
public class TestDemo {

    static OutputTag<Integer> stringOutputTag =
            new OutputTag<>("testTag", TypeInformation.of(Integer.class));

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> source =
                env.addSource(
                                new SourceFunction<Integer>() {
                                    @Override
                                    public void run(SourceContext<Integer> ctx) throws Exception {
                                        int tag = 0;
                                        while (true) {
                                            ctx.collect(tag++);
                                            Thread.sleep(1000);
                                        }
                                    }

                                    @Override
                                    public void cancel() {}
                                })
                        .setParallelism(1);

        source.map(
                        new MapFunction<Integer, Integer>() {
                            @Override
                            public Integer map(Integer value) throws Exception {
                                return value * -1;
                            }
                        })
                .setParallelism(1)
                //                .flatMap(
                //                        new FlatMapFunction<Integer, Double>() {
                //                            @Override
                //                            public void flatMap(Integer value, Collector<Double>
                // out)
                //                                    throws Exception {
                //                                out.collect(value * 0.001);
                //                            }
                //                        })
                //                .setParallelism(1)
                .print()
                .setParallelism(2);
        env.execute("test");
    }
}
