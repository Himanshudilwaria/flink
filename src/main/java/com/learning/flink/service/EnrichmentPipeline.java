package com.learning.flink.service;

import com.learning.flink.dto.EnrichedData;
import com.learning.flink.dto.FailedData;
import com.learning.flink.dto.InputData;
import com.learning.flink.dto.ReferenceData;
import com.learning.flink.service.sinkBuilder.KafkaSinkBuilder;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

@AllArgsConstructor
@NoArgsConstructor
@Slf4j
public class EnrichmentPipeline {
    private StreamExecutionEnvironment env;
    private KafkaSource<InputData> mainSource;
    private KafkaSource<ReferenceData> refSource;
    private final OutputTag<FailedData> FAILED_DATA_OUTPUT = new OutputTag<>("FAILURES_OUTPUT", TypeInformation.of(FailedData.class));
    private ParameterTool params;

    public void build() {
        log.info("Starting pipeline construction");

        DataStream<InputData> mainStream = env.fromSource(
                mainSource,
                WatermarkStrategy.noWatermarks(),
                "Main Source"
        );
        log.debug("Created main streams");

        DataStream<ReferenceData> refStream = env.fromSource(
                refSource,
                WatermarkStrategy.noWatermarks(),
                "Reference Source"
        );

        log.debug("Created reference streams");

        SingleOutputStreamOperator<EnrichedData> enrichedStream = mainStream
                .keyBy(InputData::getTagB)
                .connect(refStream.broadcast(StateDescriptors.getRefStateDescriptor()))
                .process(new EnrichmentProcessor(FAILED_DATA_OUTPUT,params))
                .name("EnrichmentProcessor");

        enrichedStream.sinkTo(KafkaSinkBuilder.buildSuccessSink(params));
        enrichedStream.getSideOutput(FAILED_DATA_OUTPUT).sinkTo(KafkaSinkBuilder.buildFailedDataSink(params));

        mainStream.print();
        refStream.print();
        enrichedStream.print("Enriched Data");

        log.info("Pipeline construction completed");
    }
}
