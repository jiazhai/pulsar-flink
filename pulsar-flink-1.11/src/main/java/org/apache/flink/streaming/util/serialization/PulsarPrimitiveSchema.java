/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.util.serialization;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import com.google.common.base.Preconditions;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.schema.BooleanSchema;
import org.apache.pulsar.client.impl.schema.ByteSchema;
import org.apache.pulsar.client.impl.schema.BytesSchema;
import org.apache.pulsar.client.impl.schema.DateSchema;
import org.apache.pulsar.client.impl.schema.DoubleSchema;
import org.apache.pulsar.client.impl.schema.FloatSchema;
import org.apache.pulsar.client.impl.schema.InstantSchema;
import org.apache.pulsar.client.impl.schema.IntSchema;
import org.apache.pulsar.client.impl.schema.LocalDateSchema;
import org.apache.pulsar.client.impl.schema.LocalDateTimeSchema;
import org.apache.pulsar.client.impl.schema.LocalTimeSchema;
import org.apache.pulsar.client.impl.schema.LongSchema;
import org.apache.pulsar.client.impl.schema.ShortSchema;
import org.apache.pulsar.client.impl.schema.TimeSchema;
import org.apache.pulsar.client.impl.schema.TimestampSchema;

import java.io.IOException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * pulsar primitive deserialization.
 */
public class PulsarPrimitiveSchema<T> implements PulsarSerializationSchema<T>,
        PulsarDeserializationSchema<T>, PulsarContextAware<T> {

    private final static Map<Class<?>, Schema<?>> pulsarPrimitives = new HashMap<>();

    private final Class<T> recordClazz;

    private final Schema<T> targetSchema;

    static {
        pulsarPrimitives.put(Boolean.class, BooleanSchema.of());
        pulsarPrimitives.put(Byte.class, ByteSchema.of());
        pulsarPrimitives.put(Short.class, ShortSchema.of());
        pulsarPrimitives.put(Integer.class, IntSchema.of());
        pulsarPrimitives.put(Long.class, LongSchema.of());
        pulsarPrimitives.put(String.class, org.apache.pulsar.client.api.Schema.STRING);
        pulsarPrimitives.put(Float.class, FloatSchema.of());
        pulsarPrimitives.put(Double.class, DoubleSchema.of());
        pulsarPrimitives.put(Byte[].class, BytesSchema.of());
        pulsarPrimitives.put(Date.class, DateSchema.of());
        pulsarPrimitives.put(Time.class, TimeSchema.of());
        pulsarPrimitives.put(Timestamp.class, TimestampSchema.of());
        pulsarPrimitives.put(LocalDate.class, LocalDateSchema.of());
        pulsarPrimitives.put(LocalTime.class, LocalTimeSchema.of());
        pulsarPrimitives.put(LocalDateTime.class, LocalDateTimeSchema.of());
        pulsarPrimitives.put(Instant.class, InstantSchema.of());
    }

    @SuppressWarnings("unchecked")
    public PulsarPrimitiveSchema(Class<T> recordClazz) {
        Preconditions.checkArgument(pulsarPrimitives.containsKey(recordClazz), "Must be of Pulsar primitive types");
        this.recordClazz = recordClazz;
        this.targetSchema = (Schema<T>) pulsarPrimitives.get(recordClazz);
    }

    @Override
    public void serialize(T element, TypedMessageBuilder<T> messageBuilder) {
        messageBuilder.value(element);
    }

    @Override
    public void deserialize(Message<T> message, Collector<T> out) throws IOException {
        out.collect(message.getValue());
    }

    @Override
    public Optional<String> getTargetTopic(T element) {
        return Optional.empty();
    }

    @Override
    public Schema<T> getSchema() {
        return targetSchema;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(recordClazz);
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }

    public static boolean isPulsarPrimitive(Class<?> key) {
        return pulsarPrimitives.containsKey(key);
    }
}
