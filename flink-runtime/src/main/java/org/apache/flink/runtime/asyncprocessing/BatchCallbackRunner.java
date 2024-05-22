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

package org.apache.flink.runtime.asyncprocessing;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.util.function.ThrowingRunnable;

import net.jcip.annotations.GuardedBy;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link org.apache.flink.core.state.StateFutureImpl.CallbackRunner} that put one mail in {@link
 * MailboxExecutor} but run multiple callbacks within one mail.
 */
public class BatchCallbackRunner {

    private static final int DEFAULT_BATCH_SIZE = 3000;

    private final MailboxExecutor mailboxExecutor;

    private final int batchSize;

    private final ConcurrentLinkedDeque<ArrayList<ThrowingRunnable<? extends Exception>>>
            callbackQueue;

    private final Object activeBufferLock = new Object();

    @GuardedBy("activeBufferLock")
    private ArrayList<ThrowingRunnable<? extends Exception>> activeBuffer;

    private final AtomicInteger currentMails = new AtomicInteger(0);

    private volatile boolean hasMail = false;

    private final Runnable newMailNotify;

    BatchCallbackRunner(MailboxExecutor mailboxExecutor, Runnable newMailNotify) {
        this.mailboxExecutor = mailboxExecutor;
        this.newMailNotify = newMailNotify;
        this.batchSize = DEFAULT_BATCH_SIZE;
        this.callbackQueue = new ConcurrentLinkedDeque<>();
        this.activeBuffer = new ArrayList<>();
    }

    public void submit(ThrowingRunnable<? extends Exception> task) {
        synchronized (activeBufferLock) {
            activeBuffer.add(task);
            if (activeBuffer.size() >= batchSize) {
                callbackQueue.offerLast(activeBuffer);
                activeBuffer = new ArrayList<>(batchSize);
            }
        }
        currentMails.incrementAndGet();
        insertMail(false);
    }

    public void insertMail(boolean force) {
        if (force || !hasMail) {
            if (currentMails.get() > 0) {
                hasMail = true;
                mailboxExecutor.execute(this::runBatch, "Batch running callback of state requests");
                notifyNewMail();
            } else {
                hasMail = false;
            }
        }
    }

    public void runBatch() throws Exception {
        ArrayList<ThrowingRunnable<? extends Exception>> batch = callbackQueue.poll();
        if (batch == null) {
            synchronized (activeBufferLock) {
                if (!activeBuffer.isEmpty()) {
                    batch = activeBuffer;
                    activeBuffer = new ArrayList<>(batchSize);
                }
            }
        }
        if (batch != null) {
            for (ThrowingRunnable<? extends Exception> task : batch) {
                task.run();
            }
            currentMails.addAndGet(-batch.size());
        }
        insertMail(true);
    }

    private void notifyNewMail() {
        if (newMailNotify != null) {
            newMailNotify.run();
        }
    }

    public boolean isHasMail() {
        return hasMail;
    }
}
