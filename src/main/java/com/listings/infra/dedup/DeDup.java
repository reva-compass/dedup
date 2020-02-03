package com.listings.infra.dedup;

import com.google.gson.JsonParser;
import com.listings.infra.dedup.utils.AvroUtils;
import com.listings.infra.dedup.utils.Utils;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DeDup {

    private static final JsonParser parser = new JsonParser();
    private static final Map<String, String> hashes = new HashMap<>();

    private final KafkaConsumer<String, byte[]> source;
    private final KafkaProducer<String, byte[]> sink;
    private final String sourceTopic;
    private final String sinkTopic;

    // TODO checknotnulls
    public DeDup(String bootStrapServers, String sourceTopic, String sinkTopic) {
        KafkaConnector connector = new KafkaConnector(props(bootStrapServers));
        this.source = connector.source(sourceTopic);
        this.sink = connector.sink();
        this.sourceTopic = sourceTopic;
        this.sinkTopic = sinkTopic;
    }

    public static void main(String[] args) throws IOException {
        // TODO all these needs to be passed in configs
        final String bootStrapServers = "b-1.listings-infra-dev-191.lguuvv.c6.kafka.us-east-1.amazonaws.com:9092," +
                "b-2.listings-infra-dev-191.lguuvv.c6.kafka.us-east-1.amazonaws.com:9092," +
                "b-3.listings-infra-dev-191.lguuvv.c6.kafka.us-east-1.amazonaws.com:9092";
        final String sourceTopic = "data_listings_mirrored_seattle_nwmls_agent";
        final String sinkTopic = "poc_test_agent";

        DeDup deDup = new DeDup(bootStrapServers, sourceTopic, sinkTopic);
        while (true) {
            ConsumerRecords<String, byte[]> records = deDup.source.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, byte[]> record : records) {
                if (record.key() == null)
                    continue;
                GenericRecord message = AvroUtils.deSerializeMessage(record.value());
                ByteBuffer bb = (ByteBuffer) message.get("payload");
                if (bb.hasArray()) {
                    String converted = new String(bb.array(), "UTF-8");
                    String uniqueId = record.key();
                    String hash = Utils.generateHash(converted);
                    if (hashes.containsKey(uniqueId)) {
                        if (!hashes.get(uniqueId).equals(hash)) {
                            // Update the new hash for unique ID
                            hashes.put(uniqueId, hash);
                            deDup.sinkMessages(uniqueId, record.value());
//                            if (uniqueId.equals("101943") || uniqueId.equals("103112") || uniqueId.equals("106778")) {
//                                System.out.println("### converted " + converted);
//                                System.out.println("### VALUE CHANGED.");
//                            }
                        }
                    } else {
                        // Add hash for unique ID
                        hashes.put(uniqueId, hash);
                        deDup.sinkMessages(uniqueId, record.value());
                    }
                }
            }
        }
    }

    private void sinkMessages(String key, byte[] val) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(sinkTopic, key, val);
        sink.send(record);
    }

    private Properties props(String bootStrapServers) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        return props;
    }

}

