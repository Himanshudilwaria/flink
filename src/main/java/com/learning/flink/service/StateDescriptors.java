package com.learning.flink.service;

import com.learning.flink.dto.InputData;
import com.learning.flink.dto.ReferenceData;
import org.apache.flink.api.common.state.MapStateDescriptor;

public class StateDescriptors {
    private static final MapStateDescriptor<String, ReferenceData> REF_STATE_DESC =
            new MapStateDescriptor<>("refDataState", String.class, ReferenceData.class);

    private static final MapStateDescriptor<String, InputData> PENDING_RETRIES_DESC =
            new MapStateDescriptor<>("pendingRetries", String.class, InputData.class);

    public static MapStateDescriptor<String, ReferenceData> getRefStateDescriptor() {
        return REF_STATE_DESC;
    }

    public static MapStateDescriptor<String, InputData> getPendingRetryStateDescriptor() {
        return PENDING_RETRIES_DESC;
    }
    

}
