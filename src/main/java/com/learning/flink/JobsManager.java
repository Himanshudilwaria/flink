package com.learning.flink;

import com.learning.flink.dto.InputData;
import com.learning.flink.dto.ReferenceData;
import com.learning.flink.service.EnrichmentPipeline;
import com.learning.flink.service.sourceBuilder.KafkaSourceBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
public class JobsManager {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromPropertiesFile(
                JobsManager.class.getClassLoader().getResourceAsStream("config.properties")
        );



        log.info("Initializing Kafka sources");
        KafkaSource<InputData> mainSource = KafkaSourceBuilder.buildMainSource(params);
        KafkaSource<ReferenceData> refSource = KafkaSourceBuilder.buildReferenceSource(params);

        new EnrichmentPipeline(env, mainSource, refSource, params).build();
        log.info("Starting Flink job execution");
        env.execute("flink-job");
    }
}
