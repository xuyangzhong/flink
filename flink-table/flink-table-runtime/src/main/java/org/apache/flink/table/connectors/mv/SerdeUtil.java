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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/** SerdeUtil. */
public class SerdeUtil {

    public static byte[] serialize(Object[] data, RowType rowType) throws IOException {
        if (data == null || rowType == null) {
            return null;
        }
        GenericRowData rowData = GenericRowData.of(data);
        TypeSerializer<RowData> keySerializer =
                InternalTypeInfo.of(rowType).createSerializer(new ExecutionConfig());
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(baos);
        keySerializer.serialize(rowData, wrapper);
        return baos.toByteArray();
    }

    public static RowData deserialize(byte[] data, RowType rowType) throws IOException {
        if (data == null || rowType == null) {
            return null;
        }
        TypeSerializer<RowData> keySerializer =
                InternalTypeInfo.of(rowType).createSerializer(new ExecutionConfig());
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputViewStreamWrapper wrapper = new DataInputViewStreamWrapper(bais);
        return keySerializer.deserialize(wrapper);
    }
}
