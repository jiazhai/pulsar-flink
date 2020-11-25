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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.formats.avro.AvroSerializationSchema;
import org.apache.flink.streaming.connectors.pulsar.TopicKeyExtractor;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.schema.BytesSchema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

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
    public void serialize(T element, TypedMessageBuilder<T> messageBuilder) {
        messageBuilder.keyBytes(topicKeyExtractor.serializeKey(element))
                .value(element);
    }

    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {
        serializationSchema.open(context);
    }

    @Override
    public Optional<String> getTargetTopic(T element) {
        return Optional.ofNullable(topicKeyExtractor.getTopic(element));
    }

    @Override
    public Schema<T> getSchema() {
        SchemaInfo si = BytesSchema.of().getSchemaInfo();
        if (serializationSchema instanceof SimpleStringSchema) {
            si = (new SchemaInfo()).setName("String").setType(SchemaType.STRING).setSchema(new byte[0]);
        } else if (serializationSchema instanceof AvroSerializationSchema) {
            final org.apache.avro.Schema schema = ((AvroSerializationSchema<T>) serializationSchema).getSchema();
            byte[] schemaBytes = schema.toString().getBytes(StandardCharsets.UTF_8);
            si.setName("Record");
            si.setSchema(schemaBytes);
            si.setType(SchemaType.AVRO);
        }
        return new FlinkSchema<>(si, serializationSchema, null);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return null;
    }
}
