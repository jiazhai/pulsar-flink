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

package org.apache.flink.streaming.connectors.pulsar;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarDeserializationSchema;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarFetcher;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarRowFetcher;
import org.apache.flink.streaming.connectors.pulsar.internal.TopicRange;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.types.Row;
import org.apache.flink.util.SerializedValue;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;

import java.util.Map;
import java.util.Properties;

/**
 * Emit Pulsar message as Row to Flink.
 */
@Slf4j
public class FlinkPulsarRowSource extends FlinkPulsarSource<Row> {

    private TypeInformation<Row> typeInformation;

    public FlinkPulsarRowSource(String adminUrl, ClientConfigurationData clientConf, Properties properties,
                                PulsarDeserializationSchema<Row> deserializer) {
        super(adminUrl, clientConf, deserializer, properties);
    }

    public FlinkPulsarRowSource(String adminUrl, ClientConfigurationData clientConf, Properties properties) {
        super(adminUrl, clientConf, (PulsarDeserializationSchema<Row>) null, properties);
    }

    public FlinkPulsarRowSource(String serviceUrl, String adminUrl, Properties properties,
                                PulsarDeserializationSchema<Row> deserializer) {
        super(serviceUrl, adminUrl, deserializer, properties);
    }

    public FlinkPulsarRowSource(String serviceUrl, String adminUrl, Properties properties) {
        super(serviceUrl, adminUrl, (PulsarDeserializationSchema<Row>) null, properties);
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        PulsarDeserializationSchema<Row> deserializer = getDeserializer();
        return deserializer.getProducedType();
    }

    protected PulsarDeserializationSchema<Row> getDeserializer() {
        return deserializer;

    }

    @Override
    protected PulsarFetcher<Row> createFetcher(
            SourceContext sourceContext,
            Map<TopicRange, MessageId> seedTopicsWithInitialOffsets,
            SerializedValue<AssignerWithPeriodicWatermarks<Row>> watermarksPeriodic,
            SerializedValue<AssignerWithPunctuatedWatermarks<Row>> watermarksPunctuated,
            ProcessingTimeService processingTimeProvider,
            long autoWatermarkInterval,
            ClassLoader userCodeClassLoader,
            StreamingRuntimeContext streamingRuntime) throws Exception {

        return new PulsarRowFetcher(
                sourceContext,
                seedTopicsWithInitialOffsets,
                watermarksPeriodic,
                watermarksPunctuated,
                processingTimeProvider,
                autoWatermarkInterval,
                userCodeClassLoader,
                streamingRuntime,
                clientConfigurationData,
                readerConf,
                pollTimeoutMs,
                deserializer,
                metadataReader);
    }
}
