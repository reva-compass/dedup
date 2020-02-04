package com.listings.infra.dedup.utils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

// TODO get AVRO utility methods from common lib
public class AvroUtils {

    public static GenericRecord deSerializeMessage(byte[] data) throws IOException {
        GenericRecord envelope = deSerializeEnvelope(data);
        ByteBuffer bb = (ByteBuffer) envelope.get("message");
        GenericRecord message = null;
        if (bb.hasArray()) {
            message = AvroUtils.deSerializePipelineMessage(bb.array());
        }
        return message;
    }

    public static GenericRecord deSerializeEnvelope(byte[] data) throws IOException {
        InputStream schemaFile = getFileFromResources("kafka_envelope.avsc");
        Schema schema = new Schema.Parser().parse(schemaFile);
        return deSerialize(data, schema);
    }

    public static GenericRecord deSerializePipelineMessage(byte[] data) throws IOException {
        InputStream schemaFile = getFileFromResources("pipeline_message_v1.avsc");
        Schema schema = new Schema.Parser().parse(schemaFile);
        return deSerialize(data, schema);
    }

    public static GenericRecord deSerialize(byte[] data, Schema schema) throws IOException {
        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
        GenericRecord result = reader.read(null, decoder);
        return result;
    }

    private static InputStream getFileFromResources(String fileName) {
        ClassLoader classLoader = AvroUtils.class.getClassLoader();
        InputStream in = classLoader.getResourceAsStream(fileName);
        if (in == null) {
            throw new IllegalArgumentException(fileName + " - file is not found!");
        } else {
            return in;
        }
    }

}
