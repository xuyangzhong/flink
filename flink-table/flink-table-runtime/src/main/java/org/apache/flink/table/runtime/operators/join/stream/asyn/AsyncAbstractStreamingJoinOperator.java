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

package org.apache.flink.table.runtime.operators.join.stream.asyn;

import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.operators.asyncprocessing.AbstractAsyncStateStreamOperator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.operators.join.JoinConditionWithNullFilters;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract asynchronous implementation for streaming unbounded Join operator which defines some
 * member fields can be shared between different implementations.
 */
public abstract class AsyncAbstractStreamingJoinOperator
        extends AbstractAsyncStateStreamOperator<RowData>
        implements TwoInputStreamOperator<RowData, RowData, RowData> {

    protected final Logger logger = LoggerFactory.getLogger(getClass());
    private static final long serialVersionUID = -376944622236540545L;

    protected static final String LEFT_RECORDS_STATE_NAME = "left-records";
    protected static final String RIGHT_RECORDS_STATE_NAME = "right-records";

    private final GeneratedJoinCondition generatedJoinCondition;
    protected final InternalTypeInfo<RowData> leftType;
    protected final InternalTypeInfo<RowData> rightType;

    protected final JoinInputSideSpec leftInputSideSpec;
    protected final JoinInputSideSpec rightInputSideSpec;

    private final boolean[] filterNullKeys;

    protected final long leftStateRetentionTime;
    protected final long rightStateRetentionTime;

    protected transient JoinConditionWithNullFilters joinCondition;
    protected transient TimestampedCollector<RowData> collector;

    public AsyncAbstractStreamingJoinOperator(
            InternalTypeInfo<RowData> leftType,
            InternalTypeInfo<RowData> rightType,
            GeneratedJoinCondition generatedJoinCondition,
            JoinInputSideSpec leftInputSideSpec,
            JoinInputSideSpec rightInputSideSpec,
            boolean[] filterNullKeys,
            long leftStateRetentionTime,
            long rightStateRetentionTime) {
        this.leftType = leftType;
        this.rightType = rightType;
        this.generatedJoinCondition = generatedJoinCondition;
        this.leftInputSideSpec = leftInputSideSpec;
        this.rightInputSideSpec = rightInputSideSpec;
        this.leftStateRetentionTime = leftStateRetentionTime;
        this.rightStateRetentionTime = rightStateRetentionTime;
        this.filterNullKeys = filterNullKeys;
        logger.info(
                "leftType {}, rightType {}, generatedJoinCondition {}, leftInputSideSpec {}, rightInputSideSpec {}, "
                        + "filterNullKeys {}, leftStateRetentionTime {}, rightStateRetentionTime {}.",
                leftType,
                rightType,
                generatedJoinCondition,
                leftInputSideSpec,
                rightInputSideSpec,
                filterNullKeys,
                leftStateRetentionTime,
                rightStateRetentionTime);
    }

    @Override
    public void open() throws Exception {
        super.open();
        JoinCondition condition =
                generatedJoinCondition.newInstance(getRuntimeContext().getUserCodeClassLoader());
        this.joinCondition = new JoinConditionWithNullFilters(condition, filterNullKeys, this);
        this.joinCondition.setRuntimeContext(getRuntimeContext());
        this.joinCondition.open(DefaultOpenContext.INSTANCE);

        this.collector = new TimestampedCollector<>(output);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (joinCondition != null) {
            joinCondition.close();
        }
    }

    /**
     * An {@link OuterRecord} is a composite of record and {@code numOfAssociations}. The {@code
     * numOfAssociations} represents the number of associated records in the other side. It is used
     * when the record is from outer side (e.g. left side in LEFT OUTER JOIN). When the {@code
     * numOfAssociations} is ZERO, we need to send a null padding row. This is useful to avoid
     * recompute the associated numbers every time.
     *
     * <p>When the record is from inner side (e.g. right side in LEFT OUTER JOIN), the {@code
     * numOfAssociations} will always be {@code -1}.
     */
    protected static final class OuterRecord {
        public final RowData record;
        public final int numOfAssociations;

        protected OuterRecord(RowData record, int numOfAssociations) {
            this.record = record;
            this.numOfAssociations = numOfAssociations;
        }
    }
}
