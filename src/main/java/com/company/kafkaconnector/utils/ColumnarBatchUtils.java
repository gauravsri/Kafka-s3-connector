package com.company.kafkaconnector.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ColumnarBatchUtils {

    public static ColumnVector createJsonStringVector(List<Map<String, Object>> data, ObjectMapper objectMapper) {
        return new JsonStringColumnVector(data, objectMapper);
    }

    private static class JsonStringColumnVector implements ColumnVector {
        private final List<String> values;
        private final ObjectMapper objectMapper;

        JsonStringColumnVector(List<Map<String, Object>> data, ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
            this.values = data.stream().map(row -> {
                try {
                    return this.objectMapper.writeValueAsString(row);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }).collect(Collectors.toList());
        }

        public DataType getDataType() {
            return StringType.STRING;
        }

        public int getSize() {
            return values.size();
        }

        public void close() {
        }

        public boolean isNullAt(int rowId) {
            return values.get(rowId) == null;
        }

        public boolean getBoolean(int rowId) {
            throw new UnsupportedOperationException();
        }

        public byte getByte(int rowId) {
            throw new UnsupportedOperationException();
        }

        public short getShort(int rowId) {
            throw new UnsupportedOperationException();
        }

        public int getInt(int rowId) {
            throw new UnsupportedOperationException();
        }

        public long getLong(int rowId) {
            throw new UnsupportedOperationException();
        }

        public float getFloat(int rowId) {
            throw new UnsupportedOperationException();
        }

        public double getDouble(int rowId) {
            throw new UnsupportedOperationException();
        }

        public String getString(int rowId) {
            return values.get(rowId);
        }

        public byte[] getBinary(int rowId) {
            throw new UnsupportedOperationException();
        }

        public java.math.BigDecimal getDecimal(int rowId) {
            throw new UnsupportedOperationException();
        }

        public MapValue getMap(int rowId) {
            throw new UnsupportedOperationException();
        }

        public ArrayValue getArray(int rowId) {
            throw new UnsupportedOperationException();
        }

        public ColumnarBatch getStruct(int rowId) {
            throw new UnsupportedOperationException();
        }
    }
}
