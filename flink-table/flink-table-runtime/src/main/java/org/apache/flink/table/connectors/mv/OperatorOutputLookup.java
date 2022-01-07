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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.runtime.operators.collect.TableCollectCoordinationRequest;
import org.apache.flink.table.runtime.operators.collect.TableCollectCoordinationResponse;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** OperatorOutputLookup. */
public class OperatorOutputLookup extends TableFunction<RowData> {
    private static final Logger LOG = LoggerFactory.getLogger(OperatorOutputLookup.class);

    private final String endpoint;
    private final String jobId;
    private final String operatorId;
    private final int parallelism;
    private final RowType rowType;
    private final RowType keyType;
    private HashMap<Integer, Long> ids;

    public OperatorOutputLookup(
            String endpoint,
            String jobId,
            String operatorId,
            int parallelism,
            RowType rowType,
            RowType keyType) {
        this.endpoint = endpoint;
        this.jobId = jobId;
        this.operatorId = operatorId;
        this.parallelism = parallelism;
        this.rowType = rowType;
        this.keyType = keyType;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        ids = new HashMap<>();
    }

    public void eval(Object... inputs) {
        try {
            doEval(inputs);
        } catch (Exception e) {
            LOG.error("Failed to execute lookup for keys: " + Arrays.toString(inputs), e);
        } finally {
            cancel();
        }
    }

    private void doEval(Object[] key) throws Exception {
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
                                true,
                                1,
                                opId.toString(),
                                subtask,
                                SerdeUtil.serialize(key, keyType),
                                keyType);
                TableCollectCoordinationResponse response =
                        (TableCollectCoordinationResponse)
                                client.sendCoordinationRequest(jId, opId, request).get();
                LOG.info("response from coordinator " + response);
                ids.putIfAbsent(subtask, response.getId());
                List<RowData> cdcLog = response.getResults(typeSerializer);
                if (cdcLog != null) {
                    cdcLog.forEach(this::collect);
                }
                if (response.isFinished()) {
                    finished.add(subtask);
                }
            }
        }
    }

    public void cancel() {
        if (ids.isEmpty()) {
            return;
        }
        try {
            FlinkClusterRestClient client =
                    new FlinkClusterRestClient(endpoint, new Configuration());
            JobID jId = JobID.fromHexString(jobId);
            OperatorID opId = OperatorID.fromJobVertexID(JobVertexID.fromHexString(operatorId));
            for (int subtask = 0; subtask < parallelism; subtask++) {
                TableCollectCoordinationRequest request =
                        new TableCollectCoordinationRequest(
                                ids.get(subtask),
                                true,
                                true,
                                1,
                                opId.toString(),
                                subtask,
                                null,
                                keyType);
                client.sendCoordinationRequest(jId, opId, request).get();
            }
        } catch (Exception ignored) {
        }
    }
}
