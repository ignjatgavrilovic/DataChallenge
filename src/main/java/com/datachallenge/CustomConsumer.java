package com.datachallenge;

import com.datachallenge.model.UniqueUsers;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.util.*;

public class CustomConsumer {

    private String consumeTopic; // topic from which log data is read
    private String produceTopic; // topic to forward to unique users data
    private String bootstrapServer; // kafka server [host:port]
    private Consumer<Long, String> consumer;
    private CustomProducer customProducer;

    private long initialTimestamp = 0;

    public CustomConsumer(String topic, String produceTopic, String bootstrapServer) {
        this.consumeTopic = topic;
        this.produceTopic = produceTopic;
        this.bootstrapServer = bootstrapServer;

        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList(topic));
    }

    public void run() throws Exception {

        customProducer = new CustomProducer(produceTopic, bootstrapServer);

        Set<String> uniqueUsersSet = new HashSet<>();
        Set<String> uniqueUsersSetTmp = new HashSet<>();

        final int giveUp = 60; // if there is no data for `giveUp` seconds, stop the consumer
        int noRecordsCount = 0;// count of seconds that had no input

        boolean fiveSecondRule = false;

        while (true) {
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);

            if (consumerRecords.count() == 0) {
                noRecordsCount++;
                System.out.println("No records count: " + noRecordsCount);
                if (noRecordsCount > giveUp) break;
                else continue;
            }

            for (ConsumerRecord<Long, String> record : consumerRecords) {
                try {
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode obj = mapper.readTree(record.value()); // read a log line

                    // get timestamp and user_id from log line
                    long timestamp = obj.findValues("timestamp").get(0).asLong();
                    String user = obj.findValuesAsText("user_id").get(0);

                    if (initialTimestamp == 0) {
                        initialTimestamp = timestamp / 60 * 60;
                    }


                    if (fiveSecondRule) {
                        if (timestamp < initialTimestamp) {
                            uniqueUsersSet.add(user);
                        } else {
                            uniqueUsersSetTmp.add(user);
                        }
                    } else {
                        uniqueUsersSet.add(user);
                    }

                    if (timestamp - initialTimestamp >= 60) {
                        initialTimestamp = timestamp / 60 * 60;
                        fiveSecondRule = true;
                        uniqueUsersSetTmp = new HashSet<>();
                    }

                    if (fiveSecondRule && timestamp - initialTimestamp > 5) {
                        fiveSecondRule = false;
                        UniqueUsers uniqueUsers = new UniqueUsers(initialTimestamp - 60, uniqueUsersSet.size());
                        customProducer.setUniqueUsers(uniqueUsers);
                        customProducer.run();
                        uniqueUsersSet = uniqueUsersSetTmp;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            consumer.commitAsync();
        }


        customProducer.close();
        consumer.close();
        System.out.println("DONE");
    }

}
