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

package org.apache.flink.streaming.runtime.operators.asyncprocessing.declare;

import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.runtime.asyncprocessing.declare.DeclarationContext;
import org.apache.flink.runtime.asyncprocessing.declare.DeclarationException;
import org.apache.flink.runtime.asyncprocessing.declare.DeclaredVariable;
import org.apache.flink.runtime.asyncprocessing.declare.NamedCallback;
import org.apache.flink.runtime.asyncprocessing.declare.NamedConsumer;
import org.apache.flink.runtime.asyncprocessing.declare.NamedFunction;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.runtime.io.RecordProcessorUtils;
import org.apache.flink.streaming.runtime.operators.asyncprocessing.AbstractAsyncStateStreamOperator;
import org.apache.flink.streaming.runtime.operators.asyncprocessing.ElementOrder;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Basic tests for {@link AbstractAsyncStateStreamOperator}. */
public class AbstractAsyncStateStreamOperatorDeclarationTest {

    protected KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String>
            createTestHarness(
                    int maxParalelism, int numSubtasks, int subtaskIndex, ElementOrder elementOrder)
                    throws Exception {
        TestDeclarationOperator testOperator = new TestDeclarationOperator(elementOrder);
        return new KeyedOneInputStreamOperatorTestHarness<>(
                testOperator,
                new TestKeySelector(),
                BasicTypeInfo.INT_TYPE_INFO,
                maxParalelism,
                numSubtasks,
                subtaskIndex);
    }

    protected KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String>
            createTestHarnessWithChain(
                    int maxParalelism, int numSubtasks, int subtaskIndex, ElementOrder elementOrder)
                    throws Exception {
        TestDeclarationChainOperator testOperator = new TestDeclarationChainOperator(elementOrder);
        return new KeyedOneInputStreamOperatorTestHarness<>(
                testOperator,
                new TestKeySelector(),
                BasicTypeInfo.INT_TYPE_INFO,
                maxParalelism,
                numSubtasks,
                subtaskIndex);
    }

    protected KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String>
            createTestHarnessWithVariable(
                    int maxParalelism, int numSubtasks, int subtaskIndex, ElementOrder elementOrder)
                    throws Exception {
        TestDeclareVariableOperator testOperator = new TestDeclareVariableOperator(elementOrder);
        return new KeyedOneInputStreamOperatorTestHarness<>(
                testOperator,
                new TestKeySelector(),
                BasicTypeInfo.INT_TYPE_INFO,
                maxParalelism,
                numSubtasks,
                subtaskIndex);
    }

    @Test
    public void testRecordProcessor() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String>
                testHarness = createTestHarness(128, 1, 0, ElementOrder.RECORD_ORDER)) {
            testHarness.open();
            TestDeclarationOperator testOperator =
                    (TestDeclarationOperator) testHarness.getOperator();
            ThrowingConsumer<StreamRecord<Tuple2<Integer, String>>, Exception> processor =
                    RecordProcessorUtils.getRecordProcessor(testOperator);
            processor.accept(new StreamRecord<>(Tuple2.of(5, "5")));
            assertThat(testOperator.getValue()).isEqualTo(12);
        }
    }

    @Test
    public void testRecordProcessorWithChain() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String>
                testHarness = createTestHarnessWithChain(128, 1, 0, ElementOrder.RECORD_ORDER)) {
            testHarness.open();
            TestDeclarationChainOperator testOperator =
                    (TestDeclarationChainOperator) testHarness.getOperator();
            ThrowingConsumer<StreamRecord<Tuple2<Integer, String>>, Exception> processor =
                    RecordProcessorUtils.getRecordProcessor(testOperator);
            processor.accept(new StreamRecord<>(Tuple2.of(5, "5")));
            assertThat(testOperator.getValue()).isEqualTo(12);
        }
    }

    @Test
    public void testRecordProcessorWithVariable() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String>
                testHarness = createTestHarnessWithVariable(128, 1, 0, ElementOrder.RECORD_ORDER)) {
            testHarness.open();
            TestDeclareVariableOperator testOperator =
                    (TestDeclareVariableOperator) testHarness.getOperator();
            ThrowingConsumer<StreamRecord<Tuple2<Integer, String>>, Exception> processor =
                    RecordProcessorUtils.getRecordProcessor(testOperator);
            processor.accept(new StreamRecord<>(Tuple2.of(5, "5")));
            assertThat(testOperator.getValue()).isEqualTo(6);
            processor.accept(new StreamRecord<>(Tuple2.of(6, "6")));
            assertThat(testOperator.getValue()).isEqualTo(13);
        }
    }

    /** A simple testing operator. */
    private static class TestDeclarationOperator extends AbstractAsyncStateStreamOperator<String>
            implements OneInputStreamOperator<Tuple2<Integer, String>, String>,
                    Triggerable<Integer, VoidNamespace>,
                    DeclarativeProcessingInput<Tuple2<Integer, String>> {

        private static final long serialVersionUID = 1L;

        private final ElementOrder elementOrder;

        final AtomicInteger value = new AtomicInteger(0);

        TestDeclarationOperator(ElementOrder elementOrder) {
            this.elementOrder = elementOrder;
        }

        @Override
        public void open() throws Exception {
            super.open();
        }

        @Override
        public ThrowingConsumer<StreamRecord<Tuple2<Integer, String>>, Exception> declareProcess(
                DeclarationContext context) throws DeclarationException {

            NamedFunction<Void, StateFuture<Integer>> adder =
                    context.declare(
                            "adder",
                            (i) -> {
                                return StateFutureUtils.completedFuture(value.incrementAndGet());
                            });
            NamedConsumer<Integer> doubler =
                    context.declare(
                            "doubler",
                            (v) -> {
                                value.addAndGet(v);
                            });
            assertThat(adder).isInstanceOf(NamedCallback.class);
            assertThat(doubler).isInstanceOf(NamedCallback.class);
            return (e) -> {
                value.addAndGet(e.getValue().f0);
                StateFutureUtils.<Void>completedVoidFuture().thenCompose(adder).thenAccept(doubler);
            };
        }

        @Override
        public ElementOrder getElementOrder() {
            return elementOrder;
        }

        @Override
        public void processElement(StreamRecord<Tuple2<Integer, String>> element) throws Exception {
            value.incrementAndGet();
        }

        @Override
        public void onEventTime(InternalTimer<Integer, VoidNamespace> timer) throws Exception {}

        @Override
        public void onProcessingTime(InternalTimer<Integer, VoidNamespace> timer)
                throws Exception {}

        public int getValue() {
            return value.get();
        }
    }

    private static class TestDeclarationChainOperator extends TestDeclarationOperator {

        TestDeclarationChainOperator(ElementOrder elementOrder) {
            super(elementOrder);
        }

        @Override
        public ThrowingConsumer<StreamRecord<Tuple2<Integer, String>>, Exception> declareProcess(
                DeclarationContext context) throws DeclarationException {

            return context.<StreamRecord<Tuple2<Integer, String>>, Void>declareChain(
                            e -> {
                                value.addAndGet(e.getValue().f0);
                                return StateFutureUtils.completedVoidFuture();
                            })
                    .thenCompose(v -> StateFutureUtils.completedFuture(value.incrementAndGet()))
                    .withName("adder")
                    .thenAccept(value::addAndGet)
                    .withName("doubler")
                    .finish();
        }
    }

    private static class TestDeclareVariableOperator extends TestDeclarationOperator {

        TestDeclareVariableOperator(ElementOrder elementOrder) {
            super(elementOrder);
        }

        @Override
        public ThrowingConsumer<StreamRecord<Tuple2<Integer, String>>, Exception> declareProcess(
                DeclarationContext context) throws DeclarationException {
            DeclaredVariable<Integer> local =
                    context.declareVariable(BasicTypeInfo.INT_TYPE_INFO, "local count", () -> 0);

            return context.<StreamRecord<Tuple2<Integer, String>>, Void>declareChain(
                            e -> {
                                local.set(e.getValue().f0);
                                return StateFutureUtils.completedVoidFuture();
                            })
                    .thenCompose(
                            v -> {
                                local.set(local.get() + 1);
                                return StateFutureUtils.completedFuture(local.get());
                            })
                    .withName("adder")
                    .thenAccept(value::addAndGet)
                    .withName("aggregate")
                    .finish();
        }
    }

    /** {@link KeySelector} for tests. */
    static class TestKeySelector implements KeySelector<Tuple2<Integer, String>, Integer> {
        private static final long serialVersionUID = 1L;

        @Override
        public Integer getKey(Tuple2<Integer, String> value) {
            return value.f0;
        }
    }
}
