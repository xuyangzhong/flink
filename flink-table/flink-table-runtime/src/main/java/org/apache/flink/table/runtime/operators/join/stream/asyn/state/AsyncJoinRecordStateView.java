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

package org.apache.flink.table.runtime.operators.join.stream.asyn.state;

import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.join.stream.state.OuterJoinRecordStateView;

/**
 * The async version of {@link OuterJoinRecordStateView}. The difference is that this class does not
 * provide iterator, but directly provides raw async state, leaving the iterator logic to the caller
 * to implement.
 */
public interface AsyncJoinRecordStateView {

    /** Add a new record to the state view. */
    StateFuture<Void> addRecord(RowData record) throws Exception;

    /** Retract the record from the state view. */
    StateFuture<Void> retractRecord(RowData record) throws Exception;

    State getRawState();

    /**
     * Adds a new record with the number of associations to the state view.
     *
     * @param record the added record
     * @param numOfAssociations the number of records associated with other side
     */
    StateFuture<Void> addRecord(RowData record, int numOfAssociations) throws Exception;

    /**
     * Updates the number of associations belongs to the record.
     *
     * @param record the record to update
     * @param numOfAssociations the new number of records associated with other side
     */
    StateFuture<Void> updateNumOfAssociations(RowData record, int numOfAssociations)
            throws Exception;
}
