package com.datachallenge;

import com.datachallenge.model.UniqueUsers;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;

public class CustomProducer {

    private String topic;
    private String bootstrapServer;
    private UniqueUsers uniqueUsers;
    private Producer<Long, String> producer;

    public CustomProducer(String topic, String bootstrapServer) {
        this.topic = topic;
        this.bootstrapServer = bootstrapServer;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        this.producer = new KafkaProducer<>(props);
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getBootstrapServer() {
        return bootstrapServer;
    }

    public void setBootstrapServer(String bootstrapServer) {
        this.bootstrapServer = bootstrapServer;
    }

    public UniqueUsers getUniqueUsers() {
        return uniqueUsers;
    }

    public void setUniqueUsers(UniqueUsers uniqueUsers) {
        this.uniqueUsers = uniqueUsers;
    }

    public void run() throws Exception {
        long time = System.currentTimeMillis();
        try {
            final ProducerRecord<Long, String> record = new ProducerRecord<>(topic, uniqueUsers.toJson());
            System.out.println(uniqueUsers.toJson());
            RecordMetadata metadata = producer.send(record).get();

            long elapsedTime = System.currentTimeMillis() - time;
            System.out.printf("sent record(key=%s value=%s) " +
                            "meta(partition=%d, offset=%d) time=%d\n",
                    record.key(), record.value(), metadata.partition(),
                    metadata.offset(), elapsedTime);
        } finally {
            producer.flush();
        }
    }

    public void close() {
        producer.close();
    }
}

