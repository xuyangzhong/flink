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
 *
 */

package org.apache.flink.table.planner.connectors;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connectors.mv.OperatorOutputSubscriber;
import org.apache.flink.table.types.logical.RowType;

/** OperatorTableSource. */
public class OperatorTableSource implements ScanTableSource {

    private final String endpoint;
    private final String jobId;
    private final String operatorId;
    private final RowType rowType;
    private final int parallelism;

    public OperatorTableSource(
            String endpoint, String jobId, String operatorId, int parallelism, RowType rowType) {
        this.endpoint = endpoint;
        this.jobId = jobId;
        this.rowType = rowType;
        this.operatorId = operatorId;
        this.parallelism = parallelism;
    }

    @Override
    public DynamicTableSource copy() {
        return new OperatorTableSource(endpoint, jobId, operatorId, parallelism, rowType);
    }

    @Override
    public String asSummaryString() {
        return "operator source";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.all();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        return SourceFunctionProvider.of(
                new OperatorOutputSubscriber(endpoint, jobId, operatorId, parallelism, rowType),
                false);
    }
}
