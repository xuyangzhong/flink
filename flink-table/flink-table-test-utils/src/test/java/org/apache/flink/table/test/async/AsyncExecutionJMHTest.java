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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.StringUtils;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.SingleShotTime;

/** Tests for async execution pattern. */
@State(Scope.Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(SingleShotTime)
@Fork(value = 1)
@Warmup(iterations = 1)
@Measurement(iterations = 10)
@Threads(1)
public class AsyncExecutionJMHTest {

    @Param({"ThenCombineProgram", "CombineAllProgram"})
    //    @Param({"CombineAllProgram"})
    String testProgram;

    @Param({"10000"})
    long dataCount;

    //    @Param({"", "async"})
    @Param({""})
    String ossObjectName;

    MiniCluster miniCluster;

    int port;

    StreamExecutionEnvironment env;

    //    @Setup(Level.Iteration)
    @Setup
    public void before() throws Exception {
        Configuration conf = new Configuration();
        conf.setString("state.backend", "org.apache.flink.state.forst.ForStStateBackendFactory");
        System.err.println("test: " + testProgram);
        if (!StringUtils.isNullOrWhitespaceOnly(ossObjectName)) {
            conf.setString(
                    "state.backend.forst.remote-dir",
                    String.format("oss://%s/%s/", System.getenv("OSS_BUCKET"), ossObjectName));
            conf.setString("fs.oss.endpoint", System.getenv("OSS_HOST"));
            conf.setString("fs.oss.accessKeyId", System.getenv("OSS_AK"));
            conf.setString("fs.oss.accessKeySecret", System.getenv("OSS_SK"));
        }

        miniCluster =
                new MiniCluster(
                        new MiniClusterConfiguration.Builder()
                                .withRandomPorts()
                                .setNumTaskManagers(2)
                                .setNumSlotsPerTaskManager(2)
                                .setConfiguration(conf)
                                .build());
        miniCluster.start();
        port = miniCluster.getRestAddress().get().getPort();
        env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", port);

        env.disableOperatorChaining();
        env.setParallelism(1);

        FileSystem.initialize(conf);
    }

    @TearDown
    public void after() throws Exception {
        env.close();
        miniCluster.close();
    }

    @Benchmark
    public void test() throws Exception {
        TestProgram2 test;
        if (testProgram.equals("ThenCombineProgram")) {
            test = new ThenCombineProgram2(dataCount, ossObjectName, env);
        } else if (testProgram.equals("CombineAllProgram")) {
            test = new CombineAllProgram2(dataCount, ossObjectName, env);
        } else {
            throw new UnsupportedOperationException();
        }

        test.run();
    }

    public static void main(String[] args) throws Exception {
        Options opts =
                new OptionsBuilder()
                        .include(AsyncExecutionJMHTest.class.getSimpleName())
                        .resultFormat(ResultFormatType.JSON)
                        // .output("src/test/resources/jmh-result.json")
                        .verbosity(VerboseMode.NORMAL)
                        .build();
        new Runner(opts).run();
    }
}
