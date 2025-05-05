package com.learning.flink.service.sourceBuilder;

import com.learning.flink.dto.InputData;
import com.learning.flink.dto.ReferenceData;
import com.learning.flink.utils.InputDataDeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.json.JsonDeserializationSchema;

public class KafkaSourceBuilder {

    public static KafkaSource<InputData> buildMainSource(ParameterTool params) {
        return KafkaSource.<InputData>builder()
                .setBootstrapServers(params.get("kafka.bootstrap.servers", "localhost:9092"))
                .setTopics(params.get("kafka.main.topic", "topicA"))
                .setGroupId(params.get("kafka.main.group.id", "main-stream"))
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(
                        new InputDataDeserializationSchema()))
                .build();
    }

    public static KafkaSource<ReferenceData> buildReferenceSource(ParameterTool params) {
        return KafkaSource.<ReferenceData>builder()
                .setBootstrapServers(params.get("kafka.bootstrap.servers", "localhost:9092"))
                .setTopics(params.get("kafka.reference.topic", "topicB"))
                .setGroupId(params.get("kafka.reference.group.id", "reference-stream"))
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(
                        new JsonDeserializationSchema<>(ReferenceData.class)))
                .build();
    }
}
