package org.apache.flink.streaming.connectors.pulsar.util;

import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import java.util.Optional;

public class DataTypeUtils {

    @SuppressWarnings("unchecked")
    public static <T> Optional<Class<T>> extractType(DataType dataType){
        if (dataType instanceof AtomicDataType){
            final Class<T> conversionClass = (Class<T>) dataType.getConversionClass();
            return Optional.of(conversionClass);
        }
        return Optional.empty();
    }
}
