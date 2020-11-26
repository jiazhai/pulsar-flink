package org.apache.flink.streaming.connectors.pulsar.util;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 获取文档字段类型的工具类
 *
 * @author Kong Xiangxin
 * Create at 2018/10/29
 */
public class TypeInformationUtils {

    /**
     * 文档字段类型缓存
     */
    private final static Map<Class<?>, List<TypeInformation<?>>> typeInfoPerDocument = new ConcurrentHashMap<>();

    private final static Map<Class<?>, TypeInformation<Row>> rowInfoPerDocument = new ConcurrentHashMap<>();

    public static TypeInformation<Row> getTypesAsRow(Class<?> documentFormat) {
        return rowInfoPerDocument.computeIfAbsent(documentFormat, documentFormat1 -> {
            List<TypeInformation<?>> columnTypes = getColumnTypes(documentFormat1);
            TypeInformation<?>[] typeInfos = columnTypes.toArray(new TypeInformation[0]);
            List<String> fieldNames = getFieldNames(documentFormat1);
            return new RowTypeInfo(typeInfos, fieldNames.toArray(new String[0]));
        });
    }

    private static List<String> getFieldNames(Class<?> documentFormat) {
        List<Field> fields = FieldUtils.getAllFieldsList(documentFormat);
        List<String> fieldNames = new ArrayList<>();
        for (Field field : fields) {
            if (Modifier.isStatic(field.getModifiers())) {
                continue;
            }
            fieldNames.add(field.getName());
        }
        return fieldNames;
    }

    public static List<TypeInformation<?>> getColumnTypes(Class<?> documentFormat) {
        return typeInfoPerDocument.computeIfAbsent(documentFormat, documentFormat1 -> {
            List<Field> fields = FieldUtils.getAllFieldsList(documentFormat1);
            List<TypeInformation<?>> typeInfos = new ArrayList<>();
            for (Field field : fields) {
                if (Modifier.isStatic(field.getModifiers())) {
                    continue;
                }
                typeInfos.add(TypeInformation.of(field.getType()));
            }
            return typeInfos;
        });
    }
}
