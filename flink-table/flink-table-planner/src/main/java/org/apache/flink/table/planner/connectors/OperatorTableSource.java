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

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

public class OperatorTableSource implements ScanTableSource {

    private final String jobId;
    private final RowType rowType;

    public OperatorTableSource(String jobId, RowType rowType) {
        this.jobId = jobId;
        this.rowType = rowType;
    }

    @Override
    public DynamicTableSource copy() {
        return new OperatorTableSource(jobId, rowType);
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
        // TODO
        return SourceFunctionProvider.of(
                new SourceFunction<RowData>() {
                    @Override
                    public void run(SourceContext<RowData> ctx) throws Exception {}

                    @Override
                    public void cancel() {}
                },
                true);
    }
}
