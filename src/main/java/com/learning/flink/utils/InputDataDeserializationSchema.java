package com.learning.flink.utils;

import com.learning.flink.dto.InputData;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class InputDataDeserializationSchema implements DeserializationSchema<InputData> {
    private final ObjectMapper jsonMapper = new ObjectMapper();
    private final XmlMapper xmlMapper = new XmlMapper();

    @Override
    public InputData deserialize(byte[] message) throws IOException {
        String content = new String(message, StandardCharsets.UTF_8).trim();
        try {
            return content.startsWith("<") ?
                    xmlMapper.readValue(content, InputData.class) :
                    jsonMapper.readValue(content, InputData.class);
        } catch (Exception e) {
            throw new IOException("Failed to deserialize: " + content, e);
        }
    }

    @Override
    public TypeInformation<InputData> getProducedType() {
        return TypeInformation.of(InputData.class);
    }

    @Override
    public void deserialize(byte[] message, Collector<InputData> out) throws IOException {
        DeserializationSchema.super.deserialize(message, out);
    }

    @Override
    public boolean isEndOfStream(InputData inputData) {
        return false;
    }

}
