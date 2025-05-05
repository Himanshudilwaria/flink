package com.learning.flink.service;

import com.learning.flink.dto.EnrichedData;
import com.learning.flink.dto.FailedData;
import com.learning.flink.dto.InputData;
import com.learning.flink.dto.ReferenceData;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

@Slf4j
public class EnrichmentProcessor extends KeyedBroadcastProcessFunction<String, InputData, ReferenceData, EnrichedData> {
    private final long RETRY_DELAY_MS;
    private final OutputTag<FailedData> failedDataOutput;
    private transient MapState<String, InputData> pendingInputs;

    public EnrichmentProcessor(OutputTag<FailedData> failedDataOutput, ParameterTool params) {
        this.failedDataOutput = failedDataOutput;
        this.RETRY_DELAY_MS = params.getLong("retry.delay.ms", 30000);
        log.info("Initialized EnrichmentProcessor with retry delay: {} ms", RETRY_DELAY_MS);
    }

    @Override
    public void open(Configuration parameters) {
        pendingInputs = getRuntimeContext().getMapState(StateDescriptors.getPendingRetryStateDescriptor());
        log.info("Initialized pendingInputs map state");
    }

    @Override
    public void processElement(InputData input, ReadOnlyContext ctx, Collector<EnrichedData> out) throws Exception {
        log.info("Processing input element with tagB: {}", input.getTagB());

        ReferenceData refData = ctx.getBroadcastState(StateDescriptors.getRefStateDescriptor()).get(input.getTagB());

        if (refData != null) {
            log.info("Found matching reference data for tagB: {}", input.getTagB());
            out.collect(new EnrichedData(input, refData.getEnrichmentData()));
        } else {
            log.info("No reference data found for tagB: {}. Registering for retry in {} ms",
                    input.getTagB(), RETRY_DELAY_MS);
            pendingInputs.put(input.getTagB(), input);
            long nextRetryTime = ctx.timerService().currentProcessingTime() + RETRY_DELAY_MS;
            ctx.timerService().registerProcessingTimeTimer(nextRetryTime);
            log.info("Registered retry timer at {} for tagB: {}", nextRetryTime, input.getTagB());
        }
    }

    @Override
    public void processBroadcastElement(ReferenceData refData, Context ctx, Collector<EnrichedData> out) throws Exception {
        log.info("Processing broadcast reference data with key: {}", refData.getKey());
        ctx.getBroadcastState(StateDescriptors.getRefStateDescriptor()).put(refData.getKey(), refData);
        log.info("Updated broadcast state with key: {}", refData.getKey());
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<EnrichedData> out) throws Exception {
        String key = ctx.getCurrentKey();
        log.info("Processing timer for key: {}", key);

        InputData input = pendingInputs.get(key);
        if (input == null) {
            log.warn("No pending input found for timer key: {}", key);
            return;
        }

        ReadOnlyBroadcastState<String, ReferenceData> refState =
                ctx.getBroadcastState(StateDescriptors.getRefStateDescriptor());
        ReferenceData refData = refState.get(key);

        if (refData != null) {
            log.info("Retry successful for key: {}", key);
            out.collect(new EnrichedData(input, refData.getEnrichmentData()));
        } else {
            log.warn("Retry failed for key: {} - no reference data available", key);
            ctx.output(failedDataOutput, new FailedData("No matching reference data after retry", input));
        }
        pendingInputs.remove(key);
        log.info("Cleaned up pending input for key: {}", key);
    }
}

