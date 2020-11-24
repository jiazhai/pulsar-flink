/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.pulsar.internal;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.connector.pulsar.PulsarContextAware;
import org.apache.flink.formats.avro.AvroSerializationSchema;
import org.apache.flink.streaming.connectors.pulsar.TopicKeyExtractor;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.nio.charset.StandardCharsets;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An interface for the deserialization of Pulsar messages.
 */
public class PulsarSerializationSchemaWrapper<T> implements PulsarSerializationSchema<T>, PulsarContextAware<T> {

    private final SerializationSchema<T> serializationSchema;

    private final TopicKeyExtractor<T> topicKeyExtractor;

    private final Class<T> recordClazz;

    public PulsarSerializationSchemaWrapper(
            SerializationSchema<T> serializationSchema,
            TopicKeyExtractor<T> topicKeyExtractor,
            Class<T> recordClazz
            ) {
        this.serializationSchema = checkNotNull(serializationSchema);
        ClosureCleaner.clean(
                topicKeyExtractor, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
        this.topicKeyExtractor = checkNotNull(topicKeyExtractor);
        this.recordClazz = checkNotNull(recordClazz);
    }

    @Override
    public void serialize(T element, TypedMessageBuilder<byte[]> messageBuilder) {
        messageBuilder.keyBytes(topicKeyExtractor.serializeKey(element))
                .value(serializationSchema.serialize(element));
    }

    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {
        serializationSchema.open(context);
    }

    @Override
    public String getTargetTopic(T element) {
        return topicKeyExtractor.getTopic(element);
    }

    @Override
    public Schema<?> getSchema() {
        if (serializationSchema instanceof SimpleStringSchema){
            return Schema.STRING;
        }
        if (serializationSchema instanceof AvroSerializationSchema){
            final org.apache.avro.Schema schema = ((AvroSerializationSchema<T>) serializationSchema).getSchema();
            byte[] schemaBytes = schema.toString().getBytes(StandardCharsets.UTF_8);
            SchemaInfo si = new SchemaInfo();
            si.setName("Record");
            si.setSchema(schemaBytes);
            si.setType(SchemaType.AVRO);
            return org.apache.pulsar.client.api.Schema.generic(si);
        }
        return Schema.BYTES;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return null;
    }
}
