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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

public class WordSource extends RichParallelSourceFunction<Tuple2<String, Long>> {

    private static final Logger LOG = LoggerFactory.getLogger(WordSource.class);

    private static final long serialVersionUID = 1L;

    private final long maxCount;

    private final int wordLen;

    private final int largest;

    private long baseNumForSubTask;

    private final long rate;

    private transient ThrottledIterator<Integer> throttledIterator;

    private transient char[] fatArray;

    private transient int emitNumber;

    private transient volatile boolean isRunning;

    public WordSource(int largest, long rate, int wordLen, long maxCount) {
        this.maxCount = maxCount;
        this.wordLen = wordLen;
        this.largest = largest;
        this.rate = rate;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.isRunning = true;
        this.emitNumber = 0;
        this.baseNumForSubTask = ((long) getRuntimeContext().getIndexOfThisSubtask()) * largest;

        Iterator<Integer> numberSourceIterator =
                new NumberSourceIterator(largest, System.currentTimeMillis());
        this.throttledIterator = new ThrottledIterator<>(numberSourceIterator, rate);

        this.fatArray = new char[wordLen];
        Random random = new Random(0);
        for (int i = 0; i < fatArray.length; i++) {
            fatArray[i] = (char) random.nextInt();
        }

        LOG.info(
                "maxCount {}, largest {}, baseNumForSubTask {}, wordLen {}, rate {}, fatArray {}",
                maxCount,
                largest,
                baseNumForSubTask,
                wordLen,
                rate,
                Arrays.hashCode(fatArray));
    }

    @Override
    public void run(SourceContext<Tuple2<String, Long>> sourceContext) throws Exception {
        while (isRunning) {
            if (maxCount < 0) {
                Integer number;
                if (emitNumber < largest) {
                    number = emitNumber++;
                } else {
                    number = throttledIterator.next();
                }
                sourceContext.collect(
                        Tuple2.of(
                                covertToString(baseNumForSubTask + number),
                                System.currentTimeMillis()));
            } else {
                isRunning = false;
                break;
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void close() {
        isRunning = false;
    }

    public static DataStreamSource<Tuple2<String, Long>> getSource(
            StreamExecutionEnvironment env, long rate, int largest, int wordLen) {
        return getSource(env, rate, largest, wordLen, -1);
    }

    private String covertToString(long number) {
        String a = String.valueOf(number);
        StringBuilder builder = new StringBuilder(wordLen);
        builder.append(a);
        builder.append(fatArray, 0, wordLen - a.length());
        return builder.toString();
    }

    public static DataStreamSource<Tuple2<String, Long>> getSource(
            StreamExecutionEnvironment env, long rate, int largest, int wordLen, long maxCount) {
        return env.addSource(new WordSource(largest, rate, wordLen, maxCount));
    }

    // ------------------------------------------------------------------------
    //  Number generator
    // ------------------------------------------------------------------------

    static class NumberSourceIterator implements Iterator<Integer>, Serializable {
        private final int largest;
        private final Random rnd;

        public NumberSourceIterator(int largest, long seed) {
            this.largest = largest;
            this.rnd = new Random(seed);
        }

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public Integer next() {
            Integer value = rnd.nextInt(largest + 1);
            return value;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
