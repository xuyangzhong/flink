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

package org.apache.flink.table.runtime.operators.aggregate.window.builder;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedNamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.operators.aggregate.window.processors.UnsliceWindowAggProcessor;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.operators.window.windowtvf.common.AbstractWindowOperator;
import org.apache.flink.table.runtime.operators.window.windowtvf.unslicing.UnsliceAssigner;
import org.apache.flink.table.runtime.operators.window.windowtvf.unslicing.UnsliceAssigners;
import org.apache.flink.table.runtime.operators.window.windowtvf.unslicing.UnslicingWindowOperator;
import org.apache.flink.table.runtime.typeutils.AbstractRowDataSerializer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@link UnslicingWindowAggOperatorBuilder} is used to build a {@link UnslicingWindowOperator}
 * for window aggregate.
 *
 * <pre>
 * UnslicingWindowAggOperatorBuilder.builder()
 *   .inputType(inputType)
 *   .keyTypes(keyFieldTypes)
 *   .unsliceAssigner(UnsliceAssigners.session(rowtimeIndex, Duration.ofSeconds(5)))
 *   .aggregate(genAggsFunction), accTypes)
 *   .build();
 * </pre>
 */
public class UnslicingWindowAggOperatorBuilder
        extends AbstractWindowAggOperatorBuilder<TimeWindow, UnslicingWindowAggOperatorBuilder> {

    public static UnslicingWindowAggOperatorBuilder builder() {
        return new UnslicingWindowAggOperatorBuilder();
    }

    private UnsliceAssigner<TimeWindow> unsliceAssigner;

    public UnslicingWindowAggOperatorBuilder unsliceAssigner(
            UnsliceAssigner<TimeWindow> unsliceAssigner) {
        this.unsliceAssigner = unsliceAssigner;
        return this;
    }

    public UnslicingWindowAggOperatorBuilder globalAggregate(
            GeneratedNamespaceAggsHandleFunction<TimeWindow> localGeneratedAggregateFunction,
            GeneratedNamespaceAggsHandleFunction<TimeWindow> globalGeneratedAggregateFunction,
            GeneratedNamespaceAggsHandleFunction<TimeWindow> stateGeneratedAggregateFunction,
            AbstractRowDataSerializer<RowData> accSerializer) {
        throw new UnsupportedOperationException(
                "Two-stage window aggregate optimization with unslicing window is not supported yet.");
    }

    @Override
    protected AbstractWindowOperator<RowData, ?> buildInner() {
        checkNotNull(unsliceAssigner);
        checkNotNull(accSerializer);
        checkNotNull(generatedAggregateFunction);

        if (unsliceAssigner instanceof UnsliceAssigners.SessionUnsliceAssigner) {
            final UnsliceWindowAggProcessor windowProcessor =
                    new UnsliceWindowAggProcessor(
                            generatedAggregateFunction,
                            unsliceAssigner,
                            accSerializer,
                            indexOfCountStart,
                            shiftTimeZone);
            return new UnslicingWindowOperator<>(windowProcessor);
        }

        throw new UnsupportedOperationException(
                "Unsupported unslice assigner: " + unsliceAssigner.getClass().getCanonicalName());
    }

    @Override
    protected UnslicingWindowAggOperatorBuilder self() {
        return this;
    }
}
