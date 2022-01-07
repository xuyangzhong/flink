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
