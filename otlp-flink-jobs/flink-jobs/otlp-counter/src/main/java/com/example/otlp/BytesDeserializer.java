package com.example.otlp;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

public class BytesDeserializer extends AbstractDeserializationSchema<byte[]> {
    @Override
    public byte[] deserialize(byte[] message) {
        return message;
    }
}
