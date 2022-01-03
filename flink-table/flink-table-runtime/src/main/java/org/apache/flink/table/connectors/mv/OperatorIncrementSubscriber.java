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

package org.apache.flink.table.connectors.mv;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.commoncollect.CommonCollectCoordinationRequest;
import org.apache.flink.streaming.api.operators.commoncollect.CommonCollectCoordinationResponse;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;

public class OperatorIncrementSubscriber
        implements SourceFunction<RowData>, ResultTypeQueryable<RowData> {

    private final String endpoint;
    private final String jobId;
    private final String operatorId;
    private final int parallelism;
    private final RowType rowType;

    public OperatorIncrementSubscriber(
            String endpoint, String jobId, String operatorId, int parallelism, RowType rowType) {
        this.endpoint = endpoint;
        this.jobId = jobId;
        this.operatorId = operatorId;
        this.parallelism = parallelism;
        this.rowType = rowType;
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        FlinkClusterRestClient client = new FlinkClusterRestClient(endpoint, new Configuration());
        JobID jId = JobID.fromHexString(jobId);
        OperatorID opId = OperatorID.fromJobVertexID(JobVertexID.fromHexString(operatorId));

        TypeSerializer<RowData> typeSerializer =
                InternalTypeInfo.of(rowType).createSerializer(new ExecutionConfig());
        while (true) {
            for (int subtask = 0; subtask < parallelism; subtask++) {
                CommonCollectCoordinationRequest request =
                        new CommonCollectCoordinationRequest(true, 1, opId.toString(), subtask);
                CommonCollectCoordinationResponse response =
                        (CommonCollectCoordinationResponse)
                                client.sendCoordinationRequest(jId, opId, request).get();
                List<RowData> cdcLog = response.getResults(typeSerializer);
                if (cdcLog != null) {
                    cdcLog.forEach(ctx::collect);
                }
            }
        }
    }

    @Override
    public void cancel() {}

    @Override
    public TypeInformation<RowData> getProducedType() {
        return InternalTypeInfo.of(rowType);
    }
}
