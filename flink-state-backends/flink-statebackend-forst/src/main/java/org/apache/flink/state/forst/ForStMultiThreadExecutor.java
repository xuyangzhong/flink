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

package org.apache.flink.state.forst;

import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import java.util.ArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/** A high performance executor. */
public class ForStMultiThreadExecutor implements Executor {

    ArrayList<ForStSingleThreadExecutor> threads;

    AtomicInteger counter = new AtomicInteger();

    ForStSingleThreadExecutor lastThread = null;

    ForStMultiThreadExecutor(int parallelism, ExecutorThreadFactory executorThreadFactory) {
        threads = new ArrayList<>(parallelism);
        for (int i = 0; i < parallelism; i++) {
            threads.add(new ForStSingleThreadExecutor(executorThreadFactory));
        }
        lastThread = threads.get(0);
    }

    @Override
    public void execute(Runnable runnable) {
        getThread().execute(runnable);
    }

    private ForStSingleThreadExecutor getThread() {
        if (!lastThread.empty()) {
            lastThread = threads.get(counter.getAndIncrement() % threads.size());
        }
        return lastThread;
    }

    public void shutdown() {
        for (ForStSingleThreadExecutor thread : threads) {
            thread.shutdown();
        }
    }
}
