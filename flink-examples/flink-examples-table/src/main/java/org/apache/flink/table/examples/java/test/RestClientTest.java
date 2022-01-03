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

package org.apache.flink.table.examples.java.test;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.operators.commoncollect.CommonCollectCoordinationRequest;
import org.apache.flink.streaming.api.operators.commoncollect.CommonCollectCoordinationResponse;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.LogicalTypeDataTypeConverter;

import java.util.Arrays;

/** . */
public class RestClientTest {
    public static void main(String[] args) throws Exception {
        MyRestClusterClient client =
                new MyRestClusterClient("http://localhost:8081", new Configuration());
        JobID jobId = JobID.fromHexString("d7013ec154ada0d0be9221ed9f88228a");
        OperatorID opId =
                OperatorID.fromJobVertexID(
                        JobVertexID.fromHexString("0a448493b4782967b150582570326227"));
        LogicalType dataType = DataTypes.INT().getLogicalType();
        DataType externalType =
                LogicalTypeDataTypeConverter.toDataType(dataType).bridgedTo(Integer.class);
        TypeSerializer<RowData> serializer =
                InternalTypeInfo.<RowData>of(dataType).createSerializer(new ExecutionConfig());

        //        UncheckpointedCollectResultBuffer<RowData> buffer =
        //                new UncheckpointedCollectResultBuffer(serializer, true);
        CommonCollectCoordinationRequest request =
                new CommonCollectCoordinationRequest(true, 1, opId.toString(), 0);
        while (true) {
            CommonCollectCoordinationResponse response =
                    (CommonCollectCoordinationResponse)
                            client.sendCoordinationRequest(jobId, opId, request).get();
            //            buffer.dealWithResponse(response, 1000);
            //            System.out.println(response.getVersion());
            //            System.out.println(response.getLastCheckpointedOffset());
            //            RowData record = buffer.next();
            //            while (record != null) {
            //                System.out.println(converter.toExternal(record));
            //                record = buffer.next();
            //            }

            System.out.println(Arrays.toString(response.getResults(serializer).toArray()));
            Thread.sleep(1000);
            request = new CommonCollectCoordinationRequest(true, 1, opId.toString(), 0);
        }
    }
}
