package org.apache.flink.streaming.util.serialization;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.pulsar.util.TypeInformationUtils;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.types.DataType;

import org.apache.pulsar.client.api.Schema;

import java.io.Serializable;

public class PulsarDeserializationSchemaBuilder<V> implements Serializable {

    private DeserializationSchema<V> valueDeserializer;

    private DataType dataType;

    private Class<V> recordClass;

    private Schema<V> pulsarSchema;

    public PulsarDeserializationSchemaBuilder() {
    }

    public PulsarDeserializationSchemaBuilder<V> setValueDeserializer(DeserializationSchema<V> valueDeserializer) {
        this.valueDeserializer = valueDeserializer;
        return this;
    }

    public PulsarDeserializationSchemaBuilder<V>  setDataType(DataType dataType) {
        this.dataType = dataType;
        return this;
    }

    public PulsarDeserializationSchemaBuilder<V>  setRecordClass(Class<V> recordClass) {
        this.recordClass = recordClass;
        return this;
    }

    public PulsarDeserializationSchema<V> build(){
        if (dataType == null){
            dataType = DataTypes.RAW(TypeInformationUtils.getTypesAsRow(recordClass)).bridgedTo(recordClass);
        }
        return new PulsarDeserializationSchemaWrapper<>(valueDeserializer, dataType);
    }
}
