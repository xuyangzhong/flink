/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package org.apache.flink.streaming.examples.wordcountasync;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.flink.streaming.examples.wordcountasync.JobConfig.FLAT_MAP_PARALLELISM;
import static org.apache.flink.streaming.examples.wordcountasync.JobConfig.JOB_NAME;
import static org.apache.flink.streaming.examples.wordcountasync.JobConfig.SHARING_GROUP;
import static org.apache.flink.streaming.examples.wordcountasync.JobConfig.TTL;
import static org.apache.flink.streaming.examples.wordcountasync.JobConfig.WORD_LENGTH;
import static org.apache.flink.streaming.examples.wordcountasync.JobConfig.WORD_NUMBER;
import static org.apache.flink.streaming.examples.wordcountasync.JobConfig.WORD_RATE;
import static org.apache.flink.streaming.examples.wordcountasync.JobConfig.configureCheckpoint;
import static org.apache.flink.streaming.examples.wordcountasync.JobConfig.getConfiguration;

public class SyncWordCount {

    private static final Logger LOG = LoggerFactory.getLogger(WordCount.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        Configuration configuration = getConfiguration(params);
        // configRemoteStateBackend(configuration);
        env.getConfig().setGlobalJobParameters(configuration);
        env.disableOperatorChaining();

        String jobName = configuration.getString(JOB_NAME);

        configureCheckpoint(env, configuration);

        String group1 = "default1";
        String group2 = "default2";
        String group3 = "default3";
        if (configuration.getBoolean(SHARING_GROUP)) {
            group1 = group2 = group3 = "default";
        }

        // configure source
        int wordNumber = configuration.getInteger(WORD_NUMBER);
        int wordLength = configuration.getInteger(WORD_LENGTH);
        int wordRate = configuration.getInteger(WORD_RATE);

        DataStream<Tuple2<String, Long>> source =
                WordSource.getSource(env, wordRate, wordNumber, wordLength)
                        .setParallelism(1)
                        .slotSharingGroup(group1)
                        .setParallelism(configuration.getInteger(FLAT_MAP_PARALLELISM));

        // configure ttl
        long ttl = configuration.get(TTL).toMillis();

        FlatMapFunction<Tuple2<String, Long>, Long> flatMapFunction =
                getFlatMapFunction(configuration, ttl);
        DataStream<Long> mapper =
                source.keyBy(0)
                        .flatMap(flatMapFunction)
                        .setParallelism(configuration.getInteger(FLAT_MAP_PARALLELISM))
                        .slotSharingGroup(group2);

        // mapper.print().setParallelism(1);
        mapper.addSink(new BlackholeSink<>())
                .slotSharingGroup(group3)
                .setParallelism(configuration.getInteger(FLAT_MAP_PARALLELISM));

        if (jobName == null) {
            env.execute();
        } else {
            env.execute(jobName);
        }
    }

    private static FlatMapFunction<Tuple2<String, Long>, Long> getFlatMapFunction(
            Configuration configuration, long ttl) {
        return new SyncMixedFlatMapper();
    }

    public static class SyncMixedFlatMapper
            extends RichFlatMapFunction<Tuple2<String, Long>, Long> {

        private transient ValueState<Integer> wordCounter;

        public SyncMixedFlatMapper() {}

        @Override
        public void flatMap(Tuple2<String, Long> in, Collector<Long> out) throws IOException {
            Integer currentValue = wordCounter.value();

            if (currentValue != null) {
                wordCounter.update(currentValue + 1);
            } else {
                wordCounter.update(1);
            }

            out.collect(in.f1);
        }

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<Integer> descriptor =
                    new ValueStateDescriptor<>(
                            "wc", TypeInformation.of(new TypeHint<Integer>() {}));
            wordCounter = getRuntimeContext().getState(descriptor);
        }
    }
}
