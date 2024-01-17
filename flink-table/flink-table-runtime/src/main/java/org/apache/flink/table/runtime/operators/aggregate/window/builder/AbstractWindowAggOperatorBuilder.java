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
import org.apache.flink.table.runtime.operators.window.windowtvf.common.AbstractWindowOperator;
import org.apache.flink.table.runtime.operators.window.windowtvf.slicing.SliceAssigners;
import org.apache.flink.table.runtime.typeutils.AbstractRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.PagedTypeSerializer;

import java.time.ZoneId;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@link AbstractWindowAggOperatorBuilder} is a base class for building window aggregate
 * operators.
 *
 * <p>See more details in {@link SlicingWindowAggOperatorBuilder}.
 *
 * <p>TODO support UnslicingWindowAggOperatorBuilder.
 *
 * @param <W> The type of the window. {@link Long} for slicing window.
 * @param <T> The implementation of the abstract builder.
 */
public abstract class AbstractWindowAggOperatorBuilder<
        W, T extends AbstractWindowAggOperatorBuilder> {

    protected AbstractRowDataSerializer<RowData> inputSerializer;
    protected PagedTypeSerializer<RowData> keySerializer;
    protected AbstractRowDataSerializer<RowData> accSerializer;
    protected GeneratedNamespaceAggsHandleFunction<W> generatedAggregateFunction;
    protected GeneratedNamespaceAggsHandleFunction<W> localGeneratedAggregateFunction;
    protected GeneratedNamespaceAggsHandleFunction<W> globalGeneratedAggregateFunction;
    protected int indexOfCountStart = -1;
    protected ZoneId shiftTimeZone;

    public T inputSerializer(AbstractRowDataSerializer<RowData> inputSerializer) {
        this.inputSerializer = inputSerializer;
        return self();
    }

    /**
     * Specify the index position of the COUNT(*) value in the accumulator buffer. This is required
     * for consuming CDC upstream to check if the window needs to output data, or for Hopping
     * windows which uses this to determine whether the window is empty and then decide whether to
     * register timer for the next window.
     *
     * @see SliceAssigners.HoppingSliceAssigner#nextTriggerWindow(long, Supplier)
     */
    public T countStarIndex(int indexOfCountStart) {
        this.indexOfCountStart = indexOfCountStart;
        return self();
    }

    public T shiftTimeZone(ZoneId shiftTimeZone) {
        this.shiftTimeZone = shiftTimeZone;
        return self();
    }

    public T keySerializer(PagedTypeSerializer<RowData> keySerializer) {
        this.keySerializer = keySerializer;
        return self();
    }

    public T aggregate(
            GeneratedNamespaceAggsHandleFunction<W> generatedAggregateFunction,
            AbstractRowDataSerializer<RowData> accSerializer) {
        this.generatedAggregateFunction = generatedAggregateFunction;
        this.accSerializer = accSerializer;
        return self();
    }

    public T globalAggregate(
            GeneratedNamespaceAggsHandleFunction<W> localGeneratedAggregateFunction,
            GeneratedNamespaceAggsHandleFunction<W> globalGeneratedAggregateFunction,
            GeneratedNamespaceAggsHandleFunction<W> stateGeneratedAggregateFunction,
            AbstractRowDataSerializer<RowData> accSerializer) {
        this.localGeneratedAggregateFunction = localGeneratedAggregateFunction;
        this.globalGeneratedAggregateFunction = globalGeneratedAggregateFunction;
        this.generatedAggregateFunction = stateGeneratedAggregateFunction;
        this.accSerializer = accSerializer;
        return self();
    }

    public AbstractWindowOperator<RowData, ?> build() {
        checkNotNull(inputSerializer);
        checkNotNull(keySerializer);
        checkNotNull(shiftTimeZone);
        return buildInner();
    }

    protected abstract AbstractWindowOperator<RowData, ?> buildInner();

    protected abstract T self();
}
