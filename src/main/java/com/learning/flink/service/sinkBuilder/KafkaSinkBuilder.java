package com.learning.flink.service.sinkBuilder;

import com.learning.flink.dto.EnrichedData;
import com.learning.flink.dto.FailedData;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.formats.json.JsonSerializationSchema;

public class KafkaSinkBuilder {

    public static KafkaSink<EnrichedData> buildSuccessSink(ParameterTool params) {
        return KafkaSink.<EnrichedData>builder()
                .setBootstrapServers(params.get("kafka.bootstrap.servers", "localhost:9092"))
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<EnrichedData>builder()
                                .setTopic(params.get("kafka.success.topic", "topicC"))
                                .setValueSerializationSchema(new JsonSerializationSchema<>())
                                .build()
                )
                .build();
    }

    public static KafkaSink<FailedData> buildFailedDataSink(ParameterTool params) {
        return KafkaSink.<FailedData>builder()
                .setBootstrapServers(params.get("kafka.bootstrap.servers", "localhost:9092"))
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<FailedData>builder()
                                .setTopic(params.get("kafka.failure.topic", "topicD"))
                                .setValueSerializationSchema(new JsonSerializationSchema<>())
                                .build()
                )
                .build();
    }
}
