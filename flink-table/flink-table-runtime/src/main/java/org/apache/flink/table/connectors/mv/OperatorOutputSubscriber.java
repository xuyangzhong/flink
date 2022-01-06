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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.collect.TableCollectCoordinationRequest;
import org.apache.flink.table.runtime.operators.collect.TableCollectCoordinationResponse;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Operator. */
public class OperatorOutputSubscriber
        implements SourceFunction<RowData>, ResultTypeQueryable<RowData> {
    protected static final Logger LOG = LoggerFactory.getLogger(OperatorOutputSubscriber.class);

    private final String endpoint;
    private final String jobId;
    private final String operatorId;
    private final int parallelism;
    private final RowType rowType;
    private final boolean isBounded;
    private final @Nullable RowData key;
    private final @Nullable RowType keyType;
    private final HashMap<Integer, Long> ids = new HashMap<>();

    public OperatorOutputSubscriber(
            String endpoint,
            String jobId,
            String operatorId,
            int parallelism,
            RowType rowType,
            boolean isBounded,
            @Nullable RowData key,
            @Nullable RowType keyType) {
        this.endpoint = endpoint;
        this.jobId = jobId;
        this.operatorId = operatorId;
        this.parallelism = parallelism;
        this.rowType = rowType;
        this.isBounded = isBounded;
        this.key = key;
        this.keyType = keyType;
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        FlinkClusterRestClient client = new FlinkClusterRestClient(endpoint, new Configuration());
        JobID jId = JobID.fromHexString(jobId);
        OperatorID opId = OperatorID.fromJobVertexID(JobVertexID.fromHexString(operatorId));

        TypeSerializer<RowData> typeSerializer =
                InternalTypeInfo.of(rowType).createSerializer(new ExecutionConfig());
        Set<Integer> finished = new HashSet<>();
        while (true) {
            if (finished.size() == parallelism) {
                // finish
                break;
            }
            for (int subtask = 0; subtask < parallelism; subtask++) {
                if (finished.contains(subtask)) {
                    continue;
                }

                TableCollectCoordinationRequest request =
                        new TableCollectCoordinationRequest(
                                ids.getOrDefault(subtask, -1L),
                                false,
                                isBounded,
                                1,
                                opId.toString(),
                                subtask,
                                key,
                                keyType);
                TableCollectCoordinationResponse response =
                        (TableCollectCoordinationResponse)
                                client.sendCoordinationRequest(jId, opId, request).get();
                LOG.info("response from coordinator " + response);
                ids.putIfAbsent(subtask, response.getId());
                List<RowData> cdcLog = response.getResults(typeSerializer);
                if (cdcLog != null) {
                    cdcLog.forEach(ctx::collect);
                }
                if (response.isFinished()) {
                    finished.add(subtask);
                }
            }
        }
    }

    @Override
    public void cancel() {
        try {
            FlinkClusterRestClient client =
                    new FlinkClusterRestClient(endpoint, new Configuration());
            JobID jId = JobID.fromHexString(jobId);
            OperatorID opId = OperatorID.fromJobVertexID(JobVertexID.fromHexString(operatorId));
            for (int subtask = 0; subtask < parallelism; subtask++) {
                TableCollectCoordinationRequest request =
                        new TableCollectCoordinationRequest(
                                ids.get(subtask), true, isBounded, 1, opId.toString(), subtask,
                                key,
                                keyType);
                client.sendCoordinationRequest(jId, opId, request).get();
            }
        } catch (Exception ignored) {
        }
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return InternalTypeInfo.of(rowType);
    }
}
