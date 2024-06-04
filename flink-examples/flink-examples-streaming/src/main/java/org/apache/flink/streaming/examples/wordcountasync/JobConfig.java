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

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.rocksdb.CompressionType;

import java.time.Duration;

/** Configurations for the job. */
public class JobConfig {

    public static final ConfigOption<String> JOB_NAME =
            ConfigOptions.key("jobName").defaultValue("WordCount").withDescription("Job name");

    // word source options ======================================

    public static final ConfigOption<Integer> WORD_NUMBER =
            ConfigOptions.key("wordNumber")
                    .defaultValue(100000)
                    .withDescription("Number of different words which will influence state size.");

    public static final ConfigOption<Integer> WORD_LENGTH =
            ConfigOptions.key("wordLength")
                    .defaultValue(16)
                    .withDescription("Length of word which will influence state size.");

    public static final ConfigOption<Integer> WORD_RATE =
            ConfigOptions.key("wordRate")
                    .defaultValue(1000000)
                    .withDescription("Rate to emit words");
    // Flat map options ===============================================

    public static final ConfigOption<Duration> TTL =
            ConfigOptions.key("stateTtl")
                    .durationType()
                    .defaultValue(Duration.ZERO)
                    .withDescription("The TTL of state.");

    public static final ConfigOption<Integer> FLAT_MAP_PARALLELISM =
            ConfigOptions.key("flatMapParallelism")
                    .defaultValue(1)
                    .withDescription("The parallelism of Flat Map operator.");

    //  checkpoint options ========================================

    public static final ConfigOption<Long> CHECKPOINT_INTERVAL =
            ConfigOptions.key("checkpointInterval")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Checkpoint interval in milliseconds, and default is Long.MAX_VALUE which"
                                    + "means checkpoint is disable.");

    public static final ConfigOption<String> CHECKPOINT_PATH =
            ConfigOptions.key("checkpointPath").noDefaultValue().withDescription("Checkpoint path");

    // state backend options ======================================

    public static final ConfigOption<String> STATE_BACKEND =
            ConfigOptions.key("stateBackend")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Type of state backend, support memory, fs, gemini, niagara, rocksdb");

    public static final ConfigOption<String> STATE_MODE =
            ConfigOptions.key("stateMode")
                    .defaultValue(StateMode.MIXED.name())
                    .withDescription("Mode of state access, support write, read, and mixed");

    public static final ConfigOption<String> ROCKSDB_COMPRESS =
            ConfigOptions.key("rocksdbCompress")
                    .defaultValue(CompressionType.SNAPPY_COMPRESSION.name())
                    .withDescription("Compression type for rocksdb.");

    public static final ConfigOption<Boolean> ROCKSDB_STATS =
            ConfigOptions.key("rocksdbStats").defaultValue(false);

    public static final ConfigOption<Boolean> SHARING_GROUP =
            ConfigOptions.key("sharingGroup")
                    .defaultValue(false)
                    .withDescription("Whether to enable sharing group");

    public static Configuration getConfiguration(ParameterTool params) {
        Configuration configuration = new Configuration();
        configuration.addAll(params.getConfiguration());

        return configuration;
    }

    public enum StateBackendType {
        MEMORY,
        FS,
        GEMINI,
        ROCKSDB
    }

    public enum StateMode {
        WRITE,
        READ,
        MIXED
    }

    public static void configureCheckpoint(
            StreamExecutionEnvironment env, Configuration configuration) {
        if (configuration.get(CHECKPOINT_INTERVAL) == null) {
            return;
        }

        env.enableCheckpointing(
                configuration.getLong(CHECKPOINT_INTERVAL), CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig()
                .enableExternalizedCheckpoints(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    }
}
