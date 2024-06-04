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

import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;

/** A high performance single-thread executor. */
public class ForStSingleThreadExecutor implements Executor, Runnable {

    final Thread thread;

    final LinkedBlockingQueue<Runnable> tasks;

    volatile boolean isRunning = true;

    ForStSingleThreadExecutor(ExecutorThreadFactory factory) {
        this.tasks = new LinkedBlockingQueue<>();
        this.thread = factory.newThread(this);
        this.thread.start();
    }

    @Override
    public void run() {
        while (isRunning) {
            try {
                Runnable first = tasks.take();
                first.run();
            } catch (InterruptedException e) {
            }
        }
    }

    @Override
    public void execute(Runnable task) {
        tasks.offer(task);
    }

    public void shutdown() {
        isRunning = false;
        thread.interrupt();
    }

    public boolean empty() {
        return tasks.size() == 0;
    }
}
