package com.datachallenge;

import com.datachallenge.model.UniqueUsers;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;

public class KafkaProducerExample {
    private static final String TOPIC = "test-output";
    private static final String BOOTSTRAP_SERVER = "localhost:9092";

    private static Producer<Long, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVER);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    public static void runProducer(List<UniqueUsers> uniqueUsers) throws Exception {
        final Producer<Long, String> producer = createProducer();
        long time = System.currentTimeMillis();
        try {
            for (UniqueUsers u : uniqueUsers) {
                final ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC, u.toJson());
                RecordMetadata metadata = producer.send(record).get();

                long elapsedTime = System.currentTimeMillis() - time;
                System.out.printf("sent record(key=%s value=%s) " +
                                "meta(partition=%d, offset=%d) time=%d\n",
                        record.key(), record.value(), metadata.partition(),
                        metadata.offset(), elapsedTime);
            }
        } finally {
            producer.flush();
            producer.close();
        }
    }
}
