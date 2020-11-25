package org.apache.flink.streaming.util.serialization;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.io.IOException;
import java.io.Serializable;

public class FlinkSchema<T> implements Schema<T>, Serializable {

    private SchemaInfo schemaInfo;

    private SerializationSchema<T> serializer;

    private DeserializationSchema<T> deserializer;

    public FlinkSchema(SchemaInfo schemaInfo, SerializationSchema<T> serializer,
                       DeserializationSchema<T> deserializer) {
        this.schemaInfo = schemaInfo;
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    @Override
    public byte[] encode(T t) {
        if (serializer == null){
            throw new UnsupportedOperationException();
        }
        return serializer.serialize(t);
    }

    @Override
    public T decode(byte[] bytes) {
        if (deserializer == null){
            throw new UnsupportedOperationException();
        }
        try {
            return deserializer.deserialize(bytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return schemaInfo;
    }

    @Override
    public Schema<T> clone() {
        return this;
    }
}
